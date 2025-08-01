import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Callable

import aio_pika
from aio_pika.exceptions import QueueEmpty
from autogpt_libs.utils.cache import thread_cached
from prisma.enums import NotificationType

from backend.data import rabbitmq
from backend.data.notifications import (
    BaseEventModel,
    BaseSummaryData,
    BaseSummaryParams,
    DailySummaryData,
    DailySummaryParams,
    NotificationEventModel,
    NotificationResult,
    NotificationTypeOverride,
    QueueType,
    SummaryParamsEventModel,
    WeeklySummaryData,
    WeeklySummaryParams,
    get_batch_delay,
    get_notif_data_type,
    get_summary_params_type,
)
from backend.data.rabbitmq import (
    AsyncRabbitMQ,
    Exchange,
    ExchangeType,
    Queue,
    RabbitMQConfig,
    SyncRabbitMQ,
)
from backend.data.user import generate_unsubscribe_link
from backend.notifications.email import EmailSender
from backend.util.logging import TruncatedLogger
from backend.util.metrics import discord_send_alert
from backend.util.retry import continuous_retry
from backend.util.service import (
    AppService,
    AppServiceClient,
    endpoint_to_sync,
    expose,
    get_service_client,
)
from backend.util.settings import Settings

logger = TruncatedLogger(logging.getLogger(__name__), "[NotificationManager]")
settings = Settings()


NOTIFICATION_EXCHANGE = Exchange(name="notifications", type=ExchangeType.TOPIC)
DEAD_LETTER_EXCHANGE = Exchange(name="dead_letter", type=ExchangeType.TOPIC)
EXCHANGES = [NOTIFICATION_EXCHANGE, DEAD_LETTER_EXCHANGE]

background_executor = ProcessPoolExecutor(max_workers=2)


def create_notification_config() -> RabbitMQConfig:
    """Create RabbitMQ configuration for notifications"""

    queues = [
        # Main notification queues
        Queue(
            name="immediate_notifications",
            exchange=NOTIFICATION_EXCHANGE,
            routing_key="notification.immediate.#",
            arguments={
                "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
                "x-dead-letter-routing-key": "failed.immediate",
            },
        ),
        Queue(
            name="admin_notifications",
            exchange=NOTIFICATION_EXCHANGE,
            routing_key="notification.admin.#",
            arguments={
                "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
                "x-dead-letter-routing-key": "failed.admin",
            },
        ),
        # Summary notification queues
        Queue(
            name="summary_notifications",
            exchange=NOTIFICATION_EXCHANGE,
            routing_key="notification.summary.#",
            arguments={
                "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
                "x-dead-letter-routing-key": "failed.summary",
            },
        ),
        # Batch Queue
        Queue(
            name="batch_notifications",
            exchange=NOTIFICATION_EXCHANGE,
            routing_key="notification.batch.#",
            arguments={
                "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
                "x-dead-letter-routing-key": "failed.batch",
            },
        ),
        # Failed notifications queue
        Queue(
            name="failed_notifications",
            exchange=DEAD_LETTER_EXCHANGE,
            routing_key="failed.#",
        ),
    ]

    return RabbitMQConfig(
        exchanges=EXCHANGES,
        queues=queues,
    )


@thread_cached
def get_db():
    from backend.executor.database import DatabaseManagerClient

    return get_service_client(DatabaseManagerClient)


@thread_cached
def get_notification_queue() -> SyncRabbitMQ:
    client = SyncRabbitMQ(create_notification_config())
    client.connect()
    return client


@thread_cached
async def get_async_notification_queue() -> AsyncRabbitMQ:
    client = AsyncRabbitMQ(create_notification_config())
    await client.connect()
    return client


def get_routing_key(event_type: NotificationType) -> str:
    strategy = NotificationTypeOverride(event_type).strategy
    """Get the appropriate routing key for an event"""
    if strategy == QueueType.IMMEDIATE:
        return f"notification.immediate.{event_type.value}"
    elif strategy == QueueType.BACKOFF:
        return f"notification.backoff.{event_type.value}"
    elif strategy == QueueType.ADMIN:
        return f"notification.admin.{event_type.value}"
    elif strategy == QueueType.BATCH:
        return f"notification.batch.{event_type.value}"
    elif strategy == QueueType.SUMMARY:
        return f"notification.summary.{event_type.value}"
    return f"notification.{event_type.value}"


def queue_notification(event: NotificationEventModel) -> NotificationResult:
    """Queue a notification - exposed method for other services to call"""
    try:
        logger.debug(f"Received Request to queue {event=}")

        exchange = "notifications"
        routing_key = get_routing_key(event.type)

        queue = get_notification_queue()
        queue.publish_message(
            routing_key=routing_key,
            message=event.model_dump_json(),
            exchange=next(ex for ex in EXCHANGES if ex.name == exchange),
        )

        return NotificationResult(
            success=True,
            message=f"Notification queued with routing key: {routing_key}",
        )

    except Exception as e:
        logger.exception(f"Error queueing notification: {e}")
        return NotificationResult(success=False, message=str(e))


async def queue_notification_async(event: NotificationEventModel) -> NotificationResult:
    """Queue a notification - exposed method for other services to call"""
    try:
        logger.debug(f"Received Request to queue {event=}")

        exchange = "notifications"
        routing_key = get_routing_key(event.type)

        queue = await get_async_notification_queue()
        await queue.publish_message(
            routing_key=routing_key,
            message=event.model_dump_json(),
            exchange=next(ex for ex in EXCHANGES if ex.name == exchange),
        )

        return NotificationResult(
            success=True,
            message=f"Notification queued with routing key: {routing_key}",
        )

    except Exception as e:
        logger.exception(f"Error queueing notification: {e}")
        return NotificationResult(success=False, message=str(e))


class NotificationManager(AppService):
    """Service for handling notifications with batching support"""

    def __init__(self):
        super().__init__()
        self.rabbitmq_config = create_notification_config()
        self.running = True
        self.email_sender = EmailSender()

    @property
    def rabbit(self) -> rabbitmq.AsyncRabbitMQ:
        """Access the RabbitMQ service. Will raise if not configured."""
        if not self.rabbitmq_service:
            raise RuntimeError("RabbitMQ not configured for this service")
        return self.rabbitmq_service

    @property
    def rabbit_config(self) -> rabbitmq.RabbitMQConfig:
        """Access the RabbitMQ config. Will raise if not configured."""
        if not self.rabbitmq_config:
            raise RuntimeError("RabbitMQ not configured for this service")
        return self.rabbitmq_config

    @classmethod
    def get_port(cls) -> int:
        return settings.config.notification_service_port

    @expose
    def queue_weekly_summary(self):
        background_executor.submit(lambda: asyncio.run(self._queue_weekly_summary()))

    async def _queue_weekly_summary(self):
        """Process weekly summary for specified notification types"""
        try:
            logger.info("Processing weekly summary queuing operation")
            processed_count = 0
            current_time = datetime.now(tz=timezone.utc)
            start_time = current_time - timedelta(days=7)
            users = get_db().get_active_user_ids_in_timerange(
                end_time=current_time.isoformat(),
                start_time=start_time.isoformat(),
            )
            for user in users:
                await self._queue_scheduled_notification(
                    SummaryParamsEventModel(
                        user_id=user,
                        type=NotificationType.WEEKLY_SUMMARY,
                        data=WeeklySummaryParams(
                            start_date=start_time,
                            end_date=current_time,
                        ),
                    ),
                )
                processed_count += 1

            logger.info(f"Processed {processed_count} weekly summaries into queue")

        except Exception as e:
            logger.exception(f"Error processing weekly summary: {e}")

    @expose
    def process_existing_batches(self, notification_types: list[NotificationType]):
        background_executor.submit(self._process_existing_batches, notification_types)

    def _process_existing_batches(self, notification_types: list[NotificationType]):
        """Process existing batches for specified notification types"""
        try:
            processed_count = 0
            current_time = datetime.now(tz=timezone.utc)

            for notification_type in notification_types:
                # Get all batches for this notification type
                batches = get_db().get_all_batches_by_type(notification_type)

                for batch in batches:
                    # Check if batch has aged out
                    oldest_message = (
                        get_db().get_user_notification_oldest_message_in_batch(
                            batch.user_id, notification_type
                        )
                    )

                    if not oldest_message:
                        # this should never happen
                        logger.error(
                            f"Batch for user {batch.user_id} and type {notification_type} has no oldest message whichshould never happen!!!!!!!!!!!!!!!!"
                        )
                        continue

                    max_delay = get_batch_delay(notification_type)

                    # If batch has aged out, process it
                    if oldest_message.created_at + max_delay < current_time:
                        recipient_email = get_db().get_user_email_by_id(batch.user_id)

                        if not recipient_email:
                            logger.error(
                                f"User email not found for user {batch.user_id}"
                            )
                            continue

                        should_send = self._should_email_user_based_on_preference(
                            batch.user_id, notification_type
                        )

                        if not should_send:
                            logger.debug(
                                f"User {batch.user_id} does not want to receive {notification_type} notifications"
                            )
                            # Clear the batch
                            get_db().empty_user_notification_batch(
                                batch.user_id, notification_type
                            )
                            continue

                        batch_data = get_db().get_user_notification_batch(
                            batch.user_id, notification_type
                        )

                        if not batch_data or not batch_data.notifications:
                            logger.error(
                                f"Batch data not found for user {batch.user_id}"
                            )
                            # Clear the batch
                            get_db().empty_user_notification_batch(
                                batch.user_id, notification_type
                            )
                            continue

                        unsub_link = generate_unsubscribe_link(batch.user_id)
                        events = []
                        for db_event in batch_data.notifications:
                            try:
                                events.append(
                                    NotificationEventModel[
                                        get_notif_data_type(db_event.type)
                                    ].model_validate(
                                        {
                                            "user_id": batch.user_id,
                                            "type": db_event.type,
                                            "data": db_event.data,
                                            "created_at": db_event.created_at,
                                        }
                                    )
                                )
                            except Exception as e:
                                logger.error(
                                    f"Error parsing notification event: {e=}, {db_event=}"
                                )
                                continue
                        logger.info(f"{events=}")

                        self.email_sender.send_templated(
                            notification=notification_type,
                            user_email=recipient_email,
                            data=events,
                            user_unsub_link=unsub_link,
                        )

                        # Clear the batch
                        get_db().empty_user_notification_batch(
                            batch.user_id, notification_type
                        )

                        processed_count += 1

            logger.info(f"Processed {processed_count} aged batches")
            return {
                "success": True,
                "processed_count": processed_count,
                "notification_types": [nt.value for nt in notification_types],
                "timestamp": current_time.isoformat(),
            }

        except Exception as e:
            logger.exception(f"Error processing batches: {e}")
            return {
                "success": False,
                "error": str(e),
                "notification_types": [nt.value for nt in notification_types],
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            }

    @expose
    async def discord_system_alert(self, content: str):
        await discord_send_alert(content)

    async def _queue_scheduled_notification(self, event: SummaryParamsEventModel):
        """Queue a scheduled notification - exposed method for other services to call"""
        try:
            logger.debug(f"Received Request to queue scheduled notification {event=}")

            exchange = "notifications"
            routing_key = get_routing_key(event.type)

            # Publish to RabbitMQ
            await self.rabbit.publish_message(
                routing_key=routing_key,
                message=event.model_dump_json(),
                exchange=next(ex for ex in EXCHANGES if ex.name == exchange),
            )

        except Exception as e:
            logger.exception(f"Error queueing notification: {e}")

    def _should_email_user_based_on_preference(
        self, user_id: str, event_type: NotificationType
    ) -> bool:
        """Check if a user wants to receive a notification based on their preferences and email verification status"""
        validated_email = get_db().get_user_email_verification(user_id)
        preference = (
            get_db()
            .get_user_notification_preference(user_id)
            .preferences.get(event_type, True)
        )
        # only if both are true, should we email this person
        return validated_email and preference

    def _gather_summary_data(
        self, user_id: str, event_type: NotificationType, params: BaseSummaryParams
    ) -> BaseSummaryData:
        """Gathers the data to build a summary notification"""

        logger.info(
            f"Gathering summary data for {user_id} and {event_type} wiht {params=}"
        )

        # total_credits_used = self.run_and_wait(
        #     get_total_credits_used(user_id, start_time, end_time)
        # )

        # total_executions = self.run_and_wait(
        #     get_total_executions(user_id, start_time, end_time)
        # )

        # most_used_agent = self.run_and_wait(
        #     get_most_used_agent(user_id, start_time, end_time)
        # )

        # execution_times = self.run_and_wait(
        #     get_execution_time(user_id, start_time, end_time)
        # )

        # runs = self.run_and_wait(
        #     get_runs(user_id, start_time, end_time)
        # )
        total_credits_used = 3.0
        total_executions = 2
        most_used_agent = {"name": "Some"}
        execution_times = [1, 2, 3]
        runs = [{"status": "COMPLETED"}, {"status": "FAILED"}]

        successful_runs = len([run for run in runs if run["status"] == "COMPLETED"])
        failed_runs = len([run for run in runs if run["status"] != "COMPLETED"])
        average_execution_time = (
            sum(execution_times) / len(execution_times) if execution_times else 0
        )
        # cost_breakdown = self.run_and_wait(
        #     get_cost_breakdown(user_id, start_time, end_time)
        # )

        cost_breakdown = {
            "agent1": 1.0,
            "agent2": 2.0,
        }

        if event_type == NotificationType.DAILY_SUMMARY and isinstance(
            params, DailySummaryParams
        ):
            return DailySummaryData(
                total_credits_used=total_credits_used,
                total_executions=total_executions,
                most_used_agent=most_used_agent["name"],
                total_execution_time=sum(execution_times),
                successful_runs=successful_runs,
                failed_runs=failed_runs,
                average_execution_time=average_execution_time,
                cost_breakdown=cost_breakdown,
                date=params.date,
            )
        elif event_type == NotificationType.WEEKLY_SUMMARY and isinstance(
            params, WeeklySummaryParams
        ):
            return WeeklySummaryData(
                total_credits_used=total_credits_used,
                total_executions=total_executions,
                most_used_agent=most_used_agent["name"],
                total_execution_time=sum(execution_times),
                successful_runs=successful_runs,
                failed_runs=failed_runs,
                average_execution_time=average_execution_time,
                cost_breakdown=cost_breakdown,
                start_date=params.start_date,
                end_date=params.end_date,
            )
        else:
            raise ValueError("Invalid event type or params")

    def _should_batch(
        self, user_id: str, event_type: NotificationType, event: NotificationEventModel
    ) -> bool:

        get_db().create_or_add_to_user_notification_batch(user_id, event_type, event)

        oldest_message = get_db().get_user_notification_oldest_message_in_batch(
            user_id, event_type
        )
        if not oldest_message:
            logger.error(
                f"Batch for user {user_id} and type {event_type} has no oldest message whichshould never happen!!!!!!!!!!!!!!!!"
            )
            return False
        oldest_age = oldest_message.created_at

        max_delay = get_batch_delay(event_type)

        if oldest_age + max_delay < datetime.now(tz=timezone.utc):
            logger.info(f"Batch for user {user_id} and type {event_type} is old enough")
            return True
        logger.info(
            f"Batch for user {user_id} and type {event_type} is not old enough: {oldest_age + max_delay} < {datetime.now(tz=timezone.utc)} max_delay={max_delay}"
        )
        return False

    def _parse_message(self, message: str) -> NotificationEventModel | None:
        try:
            event = BaseEventModel.model_validate_json(message)
            return NotificationEventModel[
                get_notif_data_type(event.type)
            ].model_validate_json(message)
        except Exception as e:
            logger.error(f"Error parsing message due to non matching schema {e}")
            return None

    def _process_admin_message(self, message: str) -> bool:
        """Process a single notification, sending to an admin, returning whether to put into the failed queue"""
        try:
            event = self._parse_message(message)
            if not event:
                return False
            logger.debug(f"Processing notification for admin: {event}")
            recipient_email = settings.config.refund_notification_email
            self.email_sender.send_templated(event.type, recipient_email, event)
            return True
        except Exception as e:
            logger.exception(f"Error processing notification for admin queue: {e}")
            return False

    def _process_immediate(self, message: str) -> bool:
        """Process a single notification immediately, returning whether to put into the failed queue"""
        try:
            event = self._parse_message(message)
            if not event:
                return False
            logger.debug(f"Processing immediate notification: {event}")

            recipient_email = get_db().get_user_email_by_id(event.user_id)
            if not recipient_email:
                logger.error(f"User email not found for user {event.user_id}")
                return False

            should_send = self._should_email_user_based_on_preference(
                event.user_id, event.type
            )
            if not should_send:
                logger.debug(
                    f"User {event.user_id} does not want to receive {event.type} notifications"
                )
                return True

            unsub_link = generate_unsubscribe_link(event.user_id)

            self.email_sender.send_templated(
                notification=event.type,
                user_email=recipient_email,
                data=event,
                user_unsub_link=unsub_link,
            )
            return True
        except Exception as e:
            logger.exception(f"Error processing notification for immediate queue: {e}")
            return False

    def _process_batch(self, message: str) -> bool:
        """Process a single notification with a batching strategy, returning whether to put into the failed queue"""
        try:
            event = self._parse_message(message)
            if not event:
                return False
            logger.info(f"Processing batch notification: {event}")

            recipient_email = get_db().get_user_email_by_id(event.user_id)
            if not recipient_email:
                logger.error(f"User email not found for user {event.user_id}")
                return False

            should_send = self._should_email_user_based_on_preference(
                event.user_id, event.type
            )
            if not should_send:
                logger.info(
                    f"User {event.user_id} does not want to receive {event.type} notifications"
                )
                return True

            should_send = self._should_batch(event.user_id, event.type, event)

            if not should_send:
                logger.info("Batch not old enough to send")
                return False
            batch = get_db().get_user_notification_batch(event.user_id, event.type)
            if not batch or not batch.notifications:
                logger.error(f"Batch not found for user {event.user_id}")
                return False
            unsub_link = generate_unsubscribe_link(event.user_id)

            batch_messages = [
                NotificationEventModel[
                    get_notif_data_type(db_event.type)
                ].model_validate(
                    {
                        "user_id": event.user_id,
                        "type": db_event.type,
                        "data": db_event.data,
                        "created_at": db_event.created_at,
                    }
                )
                for db_event in batch.notifications
            ]

            self.email_sender.send_templated(
                notification=event.type,
                user_email=recipient_email,
                data=batch_messages,
                user_unsub_link=unsub_link,
            )
            # only empty the batch if we sent the email successfully
            get_db().empty_user_notification_batch(event.user_id, event.type)
            return True
        except Exception as e:
            logger.exception(f"Error processing notification for batch queue: {e}")
            return False

    def _process_summary(self, message: str) -> bool:
        """Process a single notification with a summary strategy, returning whether to put into the failed queue"""
        try:
            logger.info(f"Processing summary notification: {message}")
            event = BaseEventModel.model_validate_json(message)
            model = SummaryParamsEventModel[
                get_summary_params_type(event.type)
            ].model_validate_json(message)

            logger.info(f"Processing summary notification: {model}")

            recipient_email = get_db().get_user_email_by_id(event.user_id)
            if not recipient_email:
                logger.error(f"User email not found for user {event.user_id}")
                return False
            should_send = self._should_email_user_based_on_preference(
                event.user_id, event.type
            )
            if not should_send:
                logger.info(
                    f"User {event.user_id} does not want to receive {event.type} notifications"
                )
                return True

            summary_data = self._gather_summary_data(
                event.user_id, event.type, model.data
            )

            unsub_link = generate_unsubscribe_link(event.user_id)

            data = NotificationEventModel(
                user_id=event.user_id,
                type=event.type,
                data=summary_data,
            )

            self.email_sender.send_templated(
                notification=event.type,
                user_email=recipient_email,
                data=data,
                user_unsub_link=unsub_link,
            )
            return True
        except Exception as e:
            logger.exception(f"Error processing notification for summary queue: {e}")
            return False

    async def _run_queue(
        self,
        queue: aio_pika.abc.AbstractQueue,
        process_func: Callable[[str], bool],
        error_queue_name: str,
    ):
        message: aio_pika.abc.AbstractMessage | None = None
        try:
            # This parameter "no_ack" is named like shit, think of it as "auto_ack"
            message = await queue.get(timeout=1.0, no_ack=False)
            result = process_func(message.body.decode())
            if result:
                await message.ack()
            else:
                await message.reject(requeue=False)

        except QueueEmpty:
            logger.debug(f"Queue {error_queue_name} empty")
        except TimeoutError:
            logger.debug(f"Queue {error_queue_name} timed out")
        except Exception as e:
            if message:
                logger.error(
                    f"Error in notification service loop, message rejected {e}"
                )
                await message.reject(requeue=False)
            else:
                logger.exception(
                    f"Error in notification service loop, message unable to be rejected, and will have to be manually removed to free space in the queue: {e=}"
                )

    @continuous_retry()
    def run_service(self):
        self.run_and_wait(self._run_service())

    async def _run_service(self):
        logger.info(f"[{self.service_name}] ⏳ Configuring RabbitMQ...")
        self.rabbitmq_service = rabbitmq.AsyncRabbitMQ(self.rabbitmq_config)
        await self.rabbitmq_service.connect()

        logger.info(f"[{self.service_name}] Started notification service")

        # Set up queue consumers
        channel = await self.rabbit.get_channel()

        immediate_queue = await channel.get_queue("immediate_notifications")
        batch_queue = await channel.get_queue("batch_notifications")

        admin_queue = await channel.get_queue("admin_notifications")

        summary_queue = await channel.get_queue("summary_notifications")

        while self.running:
            try:
                await self._run_queue(
                    queue=immediate_queue,
                    process_func=self._process_immediate,
                    error_queue_name="immediate_notifications",
                )
                await self._run_queue(
                    queue=admin_queue,
                    process_func=self._process_admin_message,
                    error_queue_name="admin_notifications",
                )
                await self._run_queue(
                    queue=batch_queue,
                    process_func=self._process_batch,
                    error_queue_name="batch_notifications",
                )
                await self._run_queue(
                    queue=summary_queue,
                    process_func=self._process_summary,
                    error_queue_name="summary_notifications",
                )
                await asyncio.sleep(0.1)
            except QueueEmpty as e:
                logger.debug(f"Queue empty: {e}")

    def cleanup(self):
        """Cleanup service resources"""
        self.running = False
        super().cleanup()
        logger.info(f"[{self.service_name}] ⏳ Disconnecting RabbitMQ...")
        self.run_and_wait(self.rabbitmq_service.disconnect())


class NotificationManagerClient(AppServiceClient):
    @classmethod
    def get_service_type(cls):
        return NotificationManager

    process_existing_batches = NotificationManager.process_existing_batches
    queue_weekly_summary = NotificationManager.queue_weekly_summary
    discord_system_alert = endpoint_to_sync(NotificationManager.discord_system_alert)
