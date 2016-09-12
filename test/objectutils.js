'use strict';


// mocha defines to avoid JSHint breakage
/* global describe, it, before, beforeEach, after, afterEach */

var assert = require('assert');

var objectutils = require('../lib/objectutils.js');

var o = {
    'a': 'b',
    'o2': {
        'e': 1,
        'r': 'abbbbc',
    },
};

describe('dot', () => {
    it('should lookup values by dotted keys', () => {
        assert.strictEqual(objectutils.dot(o, 'a'), o.a, 'top level lookup');
        assert.strictEqual(objectutils.dot(o, 'o2.e'), o.o2.e, 'dotted lookup');
    });
});


describe('match', () => {
    it('should match literal values based on dotted keys', () => {
        assert.ok(objectutils.match(o, {'a': 'b'}), 'top level lookup should match');
        assert.ok(!objectutils.match(o, {'o2.e': 2}), 'dotted lookup should match');
        assert.ok(!objectutils.match(o, {'not.a.key': 2}), 'undefined lookup should not match');
    });

    it('should strictly match values based on dotted keys', () => {
        assert.ok(objectutils.match(o, {'o2.e': 1}), 'literal value should match');
        assert.ok(!objectutils.match(o, {'o2.e': "1"}), 'string vs number should not match');
    });

    it('should match regexes against strings based on dotted keys', () => {
        assert.ok(objectutils.match(o, {'o2.r': /^ab+c/}), 'regex should match');
        assert.ok(!objectutils.match(o, {'o2.r': /^b.*$/}), 'regex should not match');
    })

    it('should match regexes against strings and literals based on dotted keys', () => {
        assert.ok(objectutils.match(o, {'o2.r': /^ab+c/, 'a': 'b'}), 'regex and literal should match');
        assert.ok(!objectutils.match(o, {'o2.r': /^b.*$/, 'a': 'c'}), 'regex should match but literal should not');
    })

    it('should not match object', () => {
        assert.ok(!objectutils.match(o, {'a': {'no': 'good'}}), 'cannot match with object as filter');
    })
});
