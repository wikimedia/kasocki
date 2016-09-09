'use strict';

//
class ExtendableError extends Error {
    constructor(message) {
        super(message);

        Error.captureStackTrace(this, this.constructor);

        Object.defineProperty(this, 'name', {
            value:          this.constructor.name,
            configurable:   true,
            enumerable:     false,
            writable:       true,
        });
    }
}

class KasockiError              extends ExtendableError {
    constructor(message) {
        super(message);

        Object.defineProperty(this, 'origin', {
            value:          'Kasocki',
            configurable:   true,
            enumerable:     false,
            writable:       true,
        });
    }
}

class InvalidTopicError         extends KasockiError { }
class TopicNotAvailableError    extends KasockiError { }
class NotSubscribedError        extends KasockiError { }
class AlreadySubscribedError    extends KasockiError { }
class AlreadyStartedError       extends KasockiError { }
class AlreadyClosingError       extends KasockiError { }
class InvalidFilterError        extends KasockiError { }
class DeserializationError      extends KasockiError { }

module.exports = {
    KasockiError:               KasockiError,
    InvalidTopicError:          InvalidTopicError,
    TopicNotAvailableError:     TopicNotAvailableError,
    NotSubscribedError:         NotSubscribedError,
    AlreadySubscribedError:     AlreadySubscribedError,
    AlreadyStartedError:        AlreadyStartedError,
    AlreadyClosingError:        AlreadyClosingError,
    InvalidFilterError:         InvalidFilterError,
    DeserializationError:       DeserializationError
};