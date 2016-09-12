'use strict';

/**
 * An ExtendableError class.
 * This will define a non enumerable this.name property that matches
 * the class name during construction.
 * The constructor takes either an error message string, or another
 * Error object.  If an Error object is given, the Error's enumerable
 * keys will be placed on this new ExtenableError, and a non enumerable
 * this.originalName will be set to the class name of the provided Error.
 */
class ExtendableError extends Error {
    /**
     * @param {string|Error} e
     * @constructor
     */
    constructor(e) {
        // Allow errors to be instantied by wrapping other errors.
        if (e instanceof Error) {
            // message is an Error, instantiate this with the message string.
            super(e.message);
            // Copy each of the Error's properties to this.
            Object.keys(e).forEach((k) => {
                this[k] = e[k];
            });
            Object.defineProperty(this, 'originalName', {
                value:          e.name,
                configurable:   true,
                enumerable:     false,
                writable:       true,
            });
        }
        // Otherwise make a new Error with message.
        else {
            super(e);
        }

        Error.captureStackTrace(this, this.constructor);

        Object.defineProperty(this, 'name', {
            value:          this.constructor.name,
            configurable:   true,
            enumerable:     false,
            writable:       true,
        });
    }
}


/**
 * An ExtendableError class that always sets this.origin to 'Kasocki',
 * and provides a toJSON method for easy serialization.
 */
class KasockiError extends ExtendableError {
    /**
     * Creates a new KasockiError with extraPropeties
     * defined.
     *
     * @param {string|Error} e
     * @param {Object extraProperties}
     * @constructor
     */
    constructor(e, extraProperties) {
        super(e);

        Object.defineProperty(this, 'origin', {
            value:          'Kasocki',
            configurable:   true,
            enumerable:     false,
            writable:       true,
        });

        // Copy each extra property to this.
        if (extraProperties) {
            Object.keys(extraProperties).forEach((k) => {
                this[k] = extraProperties[k];
            });
        }
    }

    /**
     * Returns a JSON.stringify-able object.
     * This will include properties defined by
     * ExtendableError and KasockiError, as well
     * as any extra enumerable properties that have been set.
     *
     * @return {Object}
     */
    toJSON() {
        let obj = {
            message: this.message,
            origin:  this.origin,
            name:    this.name,
            stack:   this.stack
        };

        if ('originalName' in this) {
            obj.originalName = this.originalName;
        }

        Object.keys(this).forEach((k) => {
            obj[k] = this[k];
        });

        return obj;
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
    ExtendableError:            ExtendableError,
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