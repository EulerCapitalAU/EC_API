class EC_APIError(Exception):
    """Base case for EC_API Error"""

# --- Config ----
class ConfigError(EC_APIError): ...

class ConfigInputError(ConfigError):
    def __init__(self, message: str):
        super().__init__(message)

class ConfigFormatError(ConfigError):
    def __init__(self, message: str):
        super().__init__(message)

# --- State Control ---
class StateControlError(EC_APIError): ...

class StartStateError(StateControlError):
    def __init__(self, message: str):
        super().__init__(message)


class InvalidCurrentStateError(StateControlError):
    def __init__(self, message: str):
        super().__init__(message)


class InvalidNextStateError(StateControlError):
    def __init__(self, message: str):
        super().__init__(message)

# --- Recorder ---
class RecorderError(EC_APIError):...

class RowConversionError(RecorderError):
    def __init__(self, message: str):
        super().__init__(message)

class RecorderOperationalError(RecorderError):
    # Error in write to disk or conenction
    # No Crash
    def __init__(self, message: str):
        super().__init__(message)

class RecorderCriticalError(RecorderError):
    # Require restart
    def __init__(self, message: str):
        super().__init__(message)

# --- Common ---
class DataBusError(EC_APIError):...

class DataBusRegisterError(DataBusError):
    def __init__(self, message: str):
        super().__init__(message)

# --- Transport ---
class TransportError(EC_APIError): ...


class TransportSendError(TransportError):
    def __init__(self, message: str):
        super().__init__(message)


class TransportRecvError(TransportError):
    def __init__(self, message: str):
        super().__init__(message)


class TransportConnectError(TransportError):
    def __init__(self, message: str):
        super().__init__(message)


class TransportDisconnectError(TransportError):
    def __init__(self, message: str):
        super().__init__(message)


# --- Key Extractors ---
class KeyExtractorError(EC_APIError):
    def __init__(self, message: str):
        super().__init__(message)


# --- Builders ---
class MsgBuilderError(EC_APIError):
    def __init__(self, message: str):
        super().__init__(message)


# --- Parsers ---
class MsgParserError(EC_APIError):
    def __init__(self, message: str):
        super().__init__(message)


# --- Routers ---
class RoutingError(EC_APIError): ...


class DuplicateRouterKeyError(RoutingError):
    def __init__(self, message: str):
        super().__init__(message)


class UnknownRouterKeyError(RoutingError):
    def __init__(self, message: str):
        super().__init__(message)


class UnknownSubscriptionError(RoutingError):
    def __init__(self, message: str):
        super().__init__(message)


class SubscriptionQueueMismatchError(RoutingError):
    def __init__(self, message: str):
        super().__init__(message)


class MaxSymbolsExceededError(RoutingError):
    def __init__(self, message: str):
        super().__init__(message)


class MaxSubscribersExceededError(RoutingError):
    def __init__(self, message: str):
        super().__init__(message)


class InvalidDroppingPolicy(RoutingError):
    def __init__(self, message: str):
        super().__init__(message)


# --- Symbol Registry ---
class SymbolRegistryError(EC_APIError): ...


class FailRegisterError(SymbolRegistryError):
    def __init__(self, message: str):
        super().__init__(message)


class SymbolNotInRegistryError(SymbolRegistryError):
    def __init__(self, message: str):
        super().__init__(message)


class MetaDataMissingError(SymbolRegistryError):
    def __init__(self, message: str):
        super().__init__(message)


class DuplicateSymbolError(SymbolRegistryError):
    def __init__(self, message: str):
        super().__init__(message)


# --- Subscription Manager ---
class SubscriptionError(EC_APIError): ...


# ----------- Live objects (exist in runtime)
# --- Connect ---
class ConnectError(EC_APIError): ...


class ConnectEnterError(ConnectError):
    def __init__(self, message: str):
        super().__init__(message)


class ConnectRequestError(ConnectError):
    def __init__(self, message: str):
        super().__init__(message)


class ConnectCancelledError(ConnectError):
    def __init__(self, message: str):
        super().__init__(message)


class ConnectTimeOutError(ConnectError):
    def __init__(self, message: str):
        super().__init__(message)


class DisconnectError(ConnectError):
    def __init__(self, message: str):
        super().__init__(message)


class AuthError(ConnectError):
    def __init__(self, message: str):
        super().__init__(message)


class SymbolResolutionError(ConnectError):
    def __init__(self, message: str):
        super().__init__(message)


class MissingSymbolResolutionError(ConnectError):
    def __init__(self, message: str):
        super().__init__(message)


# --- Monitor
class MonitorError(EC_APIError): ...


class MonitorTimeOutError(MonitorError):
    def __init__(self, message: str):
        super().__init__(message)


class ContractIDMissingError(MonitorError):
    def __init__(self, message: str):
        super().__init__(message)


class UnsupportedLevelError(MonitorError):
    def __init__(self, message: str):
        super().__init__(message)


class MonitorDataRequestError(MonitorError):
    def __init__(self, message: str):
        super().__init__(message)


# --- Ordering
class TradeSessionError(EC_APIError): ...


class TradeSessionTimeOutError(TradeSessionError):
    def __init__(self, message: str):
        super().__init__(message)


class TradeSessionRequestError(EC_APIError):
    def __init__(self, message: str):
        super().__init__(message)


class TradeSubscriptionMissingError(TradeSessionError):
    def __init__(self, message: str):
        super().__init__(message)


# --- LiveOrder
class LiveOrderError(EC_APIError): ...


class LiveOrderTimeOutError(LiveOrderError):
    def __init__(self, message: str):
        super().__init__(message)


class MissingOrderIDError(LiveOrderError):
    def __init__(self, message: str):
        super().__init__(message)


class LiveOrderRequestError(LiveOrderError):
    def __init__(self, message: str):
        super().__init__(message)


# --- Payload
class PayloadError(EC_APIError): ...

# Safety check error
class MissingVendorError(EC_APIError):
    def __init__(self, message: str):
        super().__init__(message)


class RiskViolationError(PayloadError):
    def __init__(self, message: str):
        super().__init__(message)


class ExecutePayloadError(PayloadError):
    def __init__(self, message: str):
        super().__init__(message)

# --- Channel
class ChannelError(EC_APIError): ...

class ChannelMissingSettingError(ChannelError):
    def __init__(self, message: str):
        super().__init__(message)
        
class ChannelSubscriptionError(ChannelError):
    def __init__(self, message: str):
        super().__init__(message)
        
class ChannelBroadcastError(ChannelError):
    def __init__(self, message: str):
        super().__init__(message)
        
class ChannelListenError(ChannelError):
    def __init__(self, message: str):
        super().__init__(message)


# --- OpStrategy
class OpStrategyError(EC_APIError): ...
