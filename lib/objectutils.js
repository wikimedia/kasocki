'use strict';

/**
 * Utility functions for working with objects.
 * Includes dotted string lookup, deserialization
 * from JSON in utf-8 buffer, sub-object
 * comparision, etc.
 */

const P = require('bluebird');
const _ = require('lodash');

/**
 * Converts a utf-8 byte buffer or a JSON string into
 * an object and returns it.
 */
function factory(data) {
    // if we are given an object Object, no-op and return it now.
    if (typeof data === 'object' && data.constructor.name === 'Object') {
        return data;
    }


    // If we are given a byte Buffer, parse it as utf-8
    if (typeof data === 'object' && data.constructor.name === 'Buffer') {
        data = data.toString('utf-8');
    }

    // If we now have a string, then assume it is a JSON string.
    if (typeof data === 'string') {
        data = JSON.parse(data);
    }

    else {
        throw new Error(
            'Could not convert data into an object.  ' +
            'Data must be a utf-8 byte buffer or a JSON string'
        );
    }

    return data;
}


/**
 * Given a string dottedKey, like 'a.b.c',
 * This will return the value in obj
 * addressed by that key.  E.g. this[a][b][c].
 * Taken from http://stackoverflow.com/a/6394168/555565.
 *
 * If the dottedKey does not exist in obj, this will return undefined.
 */
function dot(obj, dottedKey) {
    return dottedKey.split('.').reduce(
        (o,i) => {
            // If we encounter a key that does not exist,
            // just returned undefined all the way back up.
            if (o === undefined) {
                return undefined;
            }
            return o[i];
        },
        obj
    );
}


/**
 * filters should be a flat object with dotted key value pairs.
 * Each dotted key will be looked up in obj and compared
 * with the filter value.  If all given filters match, this will return true,
 * otherwise false.  A filter key is a dotted notation key lookup,
 * and a value must either be a literal value or a RegExp.
 * This will not match entire objects, only literal values.
 *
 * @param {Object} obj to match against
 * @Param {filters} TODO: document
 * @return {boolean}
 */
function match(obj, filters) {

    // if we don't have any filters, just return true;
    if (_.isEmpty(filters)) {
        return true;
    }

    // Check that every filter in filters returns true.
    return Object.keys(filters).every((key) => {
        let subject = dot(obj, key);

        // make sure we aren't targeting a non literal to match against.
        // if we are, just return false now.
        if (typeof subject !== 'string' && typeof subject !== 'number') {
            return false;
        }

        let filter = filters[key];

        // If this filter is a RegExp, then test it against the subject.
        // if it matches, then return true now.
        if (
            typeof filter === 'object'           &&
            filter.constructor.name === 'RegExp'  &&
            filter.test(subject)
        ) {
            return true;
        }

        // Otherwise just compare the filter against the subject.
        // If it matches return true now.
        else if (filter === subject) {
            return true;
        }

        // If we get this far we didn't match.  Return false.
        return false;
    });
}


module.exports = {
    factory:    factory,
    dot:        dot,
    match:      match,
};
