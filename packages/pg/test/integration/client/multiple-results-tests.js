/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

'use strict'
const assert = require('assert')
const co = require('co')

const helper = require('./test-helper')

const suite = new helper.Suite('multiple result sets')

suite.test(
  'two select results work',
  co.wrap(function* () {
    const client = new helper.Client()
    yield client.connect()

    const results = yield client.query(`SELECT 'foo'::text as name; SELECT 'bar'::text as baz`)
    assert(Array.isArray(results))

    assert.equal(results[0].fields[0].name, 'name')
    assert.deepEqual(results[0].rows, [{ name: 'foo' }])

    assert.equal(results[1].fields[0].name, 'baz')
    assert.deepEqual(results[1].rows, [{ baz: 'bar' }])

    return client.end()
  })
)

suite.test(
  'multiple selects work',
  co.wrap(function* () {
    const client = new helper.Client()
    yield client.connect()

    const text = `
  SELECT * FROM generate_series(2, 4) as foo;
  SELECT * FROM generate_series(8, 10) as bar;
  SELECT * FROM generate_series(20, 22) as baz;
  `

    const results = yield client.query(text)
    assert(Array.isArray(results))

    assert.equal(results[0].fields[0].name, 'foo')
    assert.deepEqual(results[0].rows, [{ foo: 2 }, { foo: 3 }, { foo: 4 }])

    assert.equal(results[1].fields[0].name, 'bar')
    assert.deepEqual(results[1].rows, [{ bar: 8 }, { bar: 9 }, { bar: 10 }])

    assert.equal(results[2].fields[0].name, 'baz')
    assert.deepEqual(results[2].rows, [{ baz: 20 }, { baz: 21 }, { baz: 22 }])

    assert.equal(results.length, 3)

    return client.end()
  })
)

suite.test(
  'mixed queries and statements',
  co.wrap(function* () {
    const client = new helper.Client()
    yield client.connect()

    const text = `
  CREATE TEMP TABLE weather(type text);
  INSERT INTO weather(type) VALUES ('rain');
  SELECT * FROM weather;
  `

    const results = yield client.query(text)
    assert(Array.isArray(results))
    assert.equal(results[0].command, 'CREATE')
    assert.equal(results[1].command, 'INSERT')
    assert.equal(results[2].command, 'SELECT')

    return client.end()
  })
)
