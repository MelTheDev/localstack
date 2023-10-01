import abc
import dataclasses
import json
import logging
from typing import Any, TypedDict

from localstack.aws.api.events import Target
from localstack.aws.connect import connect_to
from localstack.utils.aws.arns import parse_arn, sqs_queue_url_for_arn
from localstack.utils.aws.client_types import ServicePrincipal
from localstack.utils.json import extract_jsonpath
from localstack.utils.strings import long_uid, truncate
from localstack.utils.time import TIMESTAMP_FORMAT_TZ, timestamp

LOG = logging.getLogger(__name__)

EventBridgeEvent = TypedDict(
    "EventBridgeEvent",
    {
        "version": str,
        "id": str,
        "detail-type": str,
        "source": str,
        "account": str,
        "time": str,
        "region": str,
        "resources": list[str],
        "detail": dict,
    },
)


@dataclasses.dataclass
class EventDispatchContext:
    source_service: str
    detail_type: str
    source_arn: str
    account_id: str
    region: str
    target: Target
    detail: dict = dataclasses.field(default_factory=dict)


class EventDispatcher(abc.ABC):

    target_service: str

    def dispatch(self, context: EventDispatchContext):
        raise NotImplementedError

    def create_default_event(self, context: EventDispatchContext) -> EventBridgeEvent:
        return {
            "version": "0",
            "id": long_uid(),
            "detail-type": context.detail_type,
            "source": context.source_service,
            "account": context.account_id,
            "time": timestamp(format=TIMESTAMP_FORMAT_TZ),
            "region": context.region,
            "resources": [context.source_arn],
            "detail": context.detail,
        }

    @staticmethod
    def dispatcher_for_target(target_arn: str) -> "EventDispatcher":
        service = parse_arn(target_arn)["service"]

        # TODO: split out `send_event_to_target` into individual dispatcher classes
        if service == "sqs":
            return SqsEventDispatcher()

        return LegacyScheduledEventDispatcher()


class LegacyScheduledEventDispatcher(EventDispatcher):

    target_service = None

    def dispatch(self, context: EventDispatchContext):
        from localstack.utils.aws.message_forwarding import send_event_to_target
        from localstack.utils.collections import pick_attributes

        # TODO generate event matching aws in case no Input has been specified
        event_str = context.target.get("Input")
        event = (
            json.loads(event_str) if event_str is not None else self.create_default_event(context)
        )
        attr = pick_attributes(context.target, ["$.SqsParameters", "$.KinesisParameters"])

        try:
            LOG.debug(
                "Event rule %s sending event to target %s: %s",
                context.source_arn,
                context.target["Arn"],
                event,
            )

            send_event_to_target(
                context.target["Arn"],
                event,
                target_attributes=attr,
                role=context.target.get("RoleArn"),
                target=context.target,
                source_arn=context.source_arn,
                source_service=ServicePrincipal.events,
            )
        except Exception as e:
            LOG.error(
                "Unable to send event notification %s to target %s: %s",
                truncate(event),
                context.target,
                e,
                exc_info=e if LOG.isEnabledFor(logging.DEBUG) else None,
            )


class SqsEventDispatcher(EventDispatcher):

    target_service = "sqs"

    def dispatch(self, context: EventDispatchContext):
        if input_ := context.target.get("Input"):
            body = input_
        else:
            event = self.create_event(context)
            body = json.dumps(event)

        request = {
            "QueueUrl": self.get_queue_url(context),
            "MessageBody": body,
            **context.target.get("SqsParameters", {}),
        }

        connect_to().sqs.send_message(**request)

    def get_queue_url(self, context: EventDispatchContext) -> str:
        return sqs_queue_url_for_arn(context.target["Arn"])

    def create_event(self, context: EventDispatchContext) -> Any:
        event = self.create_default_event(context)
        if input_path := context.target.get("InputPath"):
            event = extract_jsonpath(event, input_path)

        if context.target.get("InputTransformer"):
            LOG.warning("InputTransformer is currently not supported for SQS")

        return event
