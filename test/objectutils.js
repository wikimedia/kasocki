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


describe('buildFilters', () => {
    it('should build a simple filter', () => {
        let filters = {'a.b.c': 1234};
        let built = objectutils.buildFilters(filters);
        assert.deepEqual(built, filters);
    });

    it('should build a regex filter', () => {
        let filters = {'a.b.c': '/(woo|wee)/'};
        let built = objectutils.buildFilters(filters);
        assert.deepEqual(built['a.b.c'], /(woo|wee)/, 'should convert filter to a RegExp');
    });

    it('should fail with a non object', () => {
        let filters = 'gonna fail dude';
        try {
            objectutils.builtFilters(filters);
        }
        catch (e) {
            assert.ok(e instanceof Error);
        }
    });

    it('should fail with a non string or number filter', () => {
        let filters = {'a.b.c': [1,2,3]};
        try {
            objectutils.builtFilters(filters);
        }
        catch (e) {
            assert.ok(e instanceof Error);
        }
    });

    it('should fail with a bad regex filter', () => {
        let filters = {'a.b.c': '/(dangling paren.../'};
        try {
            objectutils.builtFilters(filters);
        }
        catch (e) {
            assert.ok(e instanceof Error);
        }
    });
});