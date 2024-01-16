import base64
import functools
import json
import logging
import os
import pathlib
import time

import nextcloud_client
import pika
from dotenv import load_dotenv
from limesurveyrc2api.limesurvey import LimeSurvey
from pika.exchange_type import ExchangeType

load_dotenv()

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger(__name__)
LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "DEBUG")
F_HANDLER = logging.FileHandler("errors.log")
F_HANDLER.setLevel(logging.ERROR)
F_HANDLER.setFormatter(logging.Formatter(LOG_FORMAT))


def get_secret_or_env(filename, env):
    if filename:
        with open(filename) as f:
            return f.read().strip()
    else:
        return env


LS_URL = os.getenv("LS_URL")
LS_USER = os.getenv("LS_USER")
LS_USER_FILE = os.getenv("LS_USER_FILE")
LS_PASSWORD = os.getenv("LS_PASSWORD")
LS_PASSWORD_FILE = os.getenv("LS_PASSWORD_FILE")
assert (
    LS_URL and (LS_USER or LS_USER_FILE) and (LS_PASSWORD or LS_PASSWORD_FILE)
), "Limesurvey configuration not set"


def get_ls_user():
    return get_secret_or_env(LS_USER_FILE, LS_USER)


def get_ls_password():
    return get_secret_or_env(LS_PASSWORD_FILE, LS_PASSWORD)


MQ_USER = os.getenv("MQ_USER")
MQ_USER_FILE = os.getenv("MQ_USER_FILE")
MQ_PASSWORD = os.getenv("MQ_PASSWORD")
MQ_PASSWORD_FILE = os.getenv("MQ_PASSWORD_FILE")
MQ_HOST = os.getenv("MQ_HOST", "localhost")
MQ_PORT = os.getenv("MQ_PORT", 5672)
assert (MQ_USER or MQ_USER_FILE) and (
    MQ_PASSWORD or MQ_PASSWORD_FILE
), "MQ configuration not set"


def get_mq_user():
    return get_secret_or_env(MQ_USER_FILE, MQ_USER)


def get_mq_password():
    return get_secret_or_env(MQ_PASSWORD_FILE, MQ_PASSWORD)


NC_URL = os.getenv("NC_URL")
NC_USER = os.getenv("NC_USER")
NC_USER_FILE = os.getenv("NC_USER_FILE")
NC_PASSWORD = os.getenv("NC_PASSWORD")
NC_PASSWORD_FILE = os.getenv("NC_PASSWORD_FILE")
NC_PARENT_DIR = os.getenv("NC_PARENT_DIR", "Limesurvey")
assert (
    NC_URL and (NC_USER or NC_USER_FILE) and (NC_PASSWORD or NC_PASSWORD_FILE)
), "Nextcloud configuration not set"


def get_nc_user():
    return get_secret_or_env(NC_USER_FILE, NC_USER)


def get_nc_password():
    return get_secret_or_env(NC_PASSWORD_FILE, NC_PASSWORD)


class ExampleConsumer(object):
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, this class will stop and indicate
    that reconnection is necessary. You should look at the output, as
    there are limited reasons why the connection may be closed, which
    usually are tied to permission related issues or socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """

    EXCHANGE = "router"
    EXCHANGE_TYPE = ExchangeType.direct
    QUEUE = "msgs"
    ROUTING_KEY = "example.text"

    def __init__(self, amqp_url, ls_api, nc_api):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str amqp_url: The AMQP url to connect with

        """
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url
        self._ls_api = ls_api
        self._nc_api = nc_api
        self._consuming = False
        self._durable = True
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1

    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        LOGGER.info("Connecting to %s", self._url)
        return pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info("Connection is closing or already closed")
        else:
            LOGGER.info("Closing connection")
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :param pika.SelectConnection _unused_connection: The connection

        """
        LOGGER.info("Connection opened")
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.

        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error

        """
        LOGGER.error("Connection open failed: %s", err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning("Connection closed, reconnect necessary: %s", reason)
            self.reconnect()

    def reconnect(self):
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.

        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        """Open a new channel with RabbitMQ by issuing the Channel.Open RPC
        command. When RabbitMQ responds that the channel is open, the
        on_channel_open callback will be invoked by pika.

        """
        LOGGER.info("Creating a new channel")
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info("Channel opened")
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.info("Adding channel close callback")
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed

        """
        LOGGER.warning("Channel %i was closed: %s", channel, reason)
        self.close_connection()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info("Declaring exchange: %s", exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            durable=self._durable,
            callback=cb,
        )

    def on_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)

        """
        LOGGER.info("Exchange declared: %s", userdata)
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.info("Declaring queue %s", queue_name)
        cb = functools.partial(self.on_queue_declareok, userdata=queue_name)
        self._channel.queue_declare(
            queue=queue_name, durable=self._durable, callback=cb
        )

    def on_queue_declareok(self, _unused_frame, userdata):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.

        :param pika.frame.Method _unused_frame: The Queue.DeclareOk frame
        :param str|unicode userdata: Extra user data (queue name)

        """
        queue_name = userdata
        LOGGER.info(
            "Binding %s to %s with %s", self.EXCHANGE, queue_name, self.ROUTING_KEY
        )
        cb = functools.partial(self.on_bindok, userdata=queue_name)
        self._channel.queue_bind(
            queue_name, self.EXCHANGE, routing_key=self.ROUTING_KEY, callback=cb
        )

    def on_bindok(self, _unused_frame, userdata):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will set the prefetch count for the channel.

        :param pika.frame.Method _unused_frame: The Queue.BindOk response frame
        :param str|unicode userdata: Extra user data (queue name)

        """
        LOGGER.info("Queue bound: %s", userdata)
        self.set_qos()

    def set_qos(self):
        """This method sets up the consumer prefetch to only be delivered
        one message at a time. The consumer must acknowledge this message
        before RabbitMQ will deliver another one. You should experiment
        with different prefetch values to achieve desired performance.

        """
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok
        )

    def on_basic_qos_ok(self, _unused_frame):
        """Invoked by pika when the Basic.QoS method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method _unused_frame: The Basic.QosOk response frame

        """
        LOGGER.info("QOS set to: %d", self._prefetch_count)
        self.start_consuming()

    def start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        LOGGER.info("Issuing consumer related RPC commands")
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.QUEUE, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        LOGGER.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOGGER.info("Consumer was cancelled remotely, shutting down: %r", method_frame)
        if self._channel:
            self._channel.close()

    def _requestAnswerFiles(self, surveyId, answers_json):
        map_filetypes = {
            "xls": "xlsx",
            "pdf": "pdf",
            "csv": "csv",
            "doc": "doc",
            "json": "json",
        }
        for filetype in answers_json.get("filetypeArray", []):
            res_export = self._ls_api.query(
                "export_responses",
                [
                    # @param string $sSessionKey Auth credentials
                    self._ls_api.session_key,
                    # @param int $iSurveyID ID of the Survey
                    surveyId,
                    # @param string $sDocumentType any format available by plugins (for example : pdf, csv, xls, doc, json)
                    filetype,
                    # @param string $sLanguageCode (optional) The language to be used
                    None,
                    # @param string $sCompletionStatus (optional) 'complete','incomplete' or 'all' - defaults to 'all'
                    "complete",
                    # @param string $sHeadingType (optional) 'code','full' or 'abbreviated' Optional defaults to 'code'
                    "full",
                    # @param string $sResponseType (optional)'short' or 'long' Optional defaults to 'short'
                    "long",
                    # @param integer $iFromResponseID (optional) Frpm response id
                    None,
                    # @param integer $iToResponseID (optional) To response id
                    None,
                    # @param array $aFields (optional) Name the fields to export
                    answers_json.get("questionArray", None),
                    # @param array $aAdditionalOptions (optional) Addition options for export, mainly 'convertY', 'convertN', 'nValue', 'yValue',
                    None,
                ],
            )
            pathlib.Path(str(surveyId)).mkdir(parents=True, exist_ok=True)
            localfilename = f"{answers_json.get('filename', 'answers')}.{map_filetypes.get(filetype)}"
            localfile = f"{str(surveyId)}/{localfilename}"
            with open(localfile, "wb") as f:
                f.write(base64.b64decode(res_export))

    def _requestStatisticFiles(self, surveyId, statistics_json):
        map_filetypes = {
            "xls": "xls",
            "pdf": "pdf",
            "html": "html",
        }
        for filetype in statistics_json.get("filetypeArray", []):
            res_export = self._ls_api.query(
                "export_statistics",
                [
                    # @param string $sSessionKey Auth credentials
                    self._ls_api.session_key,
                    # @param int $iSurveyID ID of the Survey
                    surveyId,
                    # @param string $docType (optional) Type of documents the exported statistics should be (pdf|xls|html)
                    filetype,
                    # @param string $sLanguage (optional) language of the survey to use (default from Survey)
                    None,
                    # @param string $graph (optional) Create graph option (default : no)
                    statistics_json.get("graph", None),
                    # @param int|array $groupIDs (optional) array or integer containing the groups we choose to generate statistics from
                    None,
                ],
            )
            pathlib.Path(str(surveyId)).mkdir(parents=True, exist_ok=True)
            localfilename = f"{statistics_json.get('filename', 'answers')}.{map_filetypes.get(filetype)}"
            localfile = f"{str(surveyId)}/{localfilename}"
            with open(localfile, "wb") as f:
                f.write(base64.b64decode(res_export))

    def _uploadFilesToNC(self, surveyId, ncDir):
        localfolder = str(surveyId)
        for file in os.listdir(localfolder):
            # filename, ext = os.path.splitext(file)
            # fileWithDate = f"{filename}_{time.strftime('%Y-%m-%d_%Hh%Mm')}{ext}"
            self._nc_api.put_file(ncDir + "/" + file, localfolder + "/" + file)

    def _removeLocalFiles(self, surveyId):
        sSurveyId = str(surveyId)
        for file in os.listdir(sSurveyId):
            localfile = f"{sSurveyId}/{file}"
            try:
                os.remove(localfile)
            except Exception as e:
                LOGGER.error("Could not remove local file", exc_info=True)
        try:
            os.rmdir(sSurveyId)
        except Exception as e:
            LOGGER.error("Could not remove local file", exc_info=True)

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel _unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param bytes body: The message body

        """
        LOGGER.info(
            "Received message # %s from %s: %s",
            basic_deliver.delivery_tag,
            properties.app_id,
            body,
        )
        # Acknowledge the message
        self.acknowledge_message(basic_deliver.delivery_tag)

        # Parse json from mq
        try:
            received_json = json.loads(body)
        except json.decoder.JSONDecodeError:
            LOGGER.error("No valid JSON in message body: %s", body)
            return
        if received_json.get("id") is None:
            LOGGER.error("No id set in message body: %s", body)
            return

        surveyId = received_json.get("id")
        sSurveyId = str(surveyId)

        # Limesurvey requests
        try:
            self._ls_api.open(password=get_ls_password())
            res_survey = self._ls_api.query(
                "get_language_properties",
                [
                    self._ls_api.session_key,
                    surveyId,
                ],
            )
            surveyName = res_survey.get("surveyls_title")

            # Request Answer Files
            answers_json = received_json.get("answers", None)
            if answers_json is not None:
                self._requestAnswerFiles(surveyId, answers_json)

            # Request Statistic Files
            statistics_json = received_json.get("statistics", None)
            if statistics_json is not None:
                self._requestStatisticFiles(surveyId, statistics_json)

            self._ls_api.close()
        except Exception as e:
            LOGGER.error("Limesurvey not reachable", exc_info=True)
            return

        # Nextcloud request
        try:
            self._nc_api.login(get_nc_user(), get_nc_password())
            dirs = self._nc_api.list(NC_PARENT_DIR)
            folderName = f"{surveyName} ({sSurveyId})"
            currentDir = NC_PARENT_DIR + "/" + folderName + "/"
            folderExistsNC = False
            for dir in dirs:
                if dir.name == folderName:
                    folderExistsNC = True
                    break
            if not folderExistsNC:
                self._nc_api.mkdir(currentDir)

            self._uploadFilesToNC(surveyId, currentDir)

            self._nc_api.logout()
        except Exception as e:
            LOGGER.error("Nextcloud not reachable", exc_info=True)
            # No return so it tries to delete the local file

        # Remove local file
        self._removeLocalFiles(surveyId)

    def acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        LOGGER.info("Acknowledging message %s", delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            LOGGER.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            cb = functools.partial(self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, userdata):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method _unused_frame: The Basic.CancelOk frame
        :param str|unicode userdata: Extra user data (consumer tag)

        """
        self._consuming = False
        LOGGER.info(
            "RabbitMQ acknowledged the cancellation of the consumer: %s", userdata
        )
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        LOGGER.info("Closing the channel")
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        if not self._closing:
            self._closing = True
            LOGGER.info("Stopping")
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            LOGGER.info("Stopped")


class ReconnectingExampleConsumer(object):
    """This is an example consumer that will reconnect if the nested
    ExampleConsumer indicates that a reconnect is necessary.

    """

    def __init__(self, amqp_url, ls_api, nc_api):
        self._reconnect_delay = 0
        self._amqp_url = amqp_url
        self._ls_api = ls_api
        self._nc_api = nc_api
        self._consumer = ExampleConsumer(self._amqp_url, self._ls_api, self._nc_api)

    def run(self):
        while True:
            try:
                self._consumer.run()
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info("Reconnecting after %d seconds", reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = ExampleConsumer(self._amqp_url, self._ls_api, self._nc_api)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


def main():
    LOGGER.addHandler(F_HANDLER)
    logging.basicConfig(level=LOGGING_LEVEL, format=LOG_FORMAT)
    ls_api = LimeSurvey(url=LS_URL, username=get_ls_user())
    nc_api = nextcloud_client.Client(NC_URL)
    amqp_url = f"amqp://{get_mq_user()}:{get_mq_password()}@{MQ_HOST}:{MQ_PORT}/"
    consumer = ReconnectingExampleConsumer(amqp_url, ls_api, nc_api)
    consumer.run()


if __name__ == "__main__":
    main()


"""
id
submitdate
lastpage
startlanguage
seed
startdate
datestamp
"""
