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
var helper = require('./test-helper')
var pg = helper.pg

var suite = new helper.Suite()

// clear process.env
var realEnv = {}
for (var key in process.env) {
  realEnv[key] = process.env[key]
  if (!key.indexOf('PG')) delete process.env[key]
}

suite.test('default values are used in new clients', function () {
  assert.same(pg.defaults, {
    user: process.env.USER,
    database: undefined,
    password: null,
    port: 5432,
    rows: 0,
    max: 10,
    binary: false,
    idleTimeoutMillis: 30000,
    client_encoding: '',
    ssl: false,
    application_name: undefined,
    fallback_application_name: undefined,
    parseInputDatesAsUTC: false,
  })

  var client = new pg.Client()
  assert.same(client, {
    user: process.env.USER,
    database: process.env.USER,
    password: null,
    port: 5432,
  })
})

suite.test('modified values are passed to created clients', function () {
  pg.defaults.user = 'boom'
  pg.defaults.password = 'zap'
  pg.defaults.database = 'pow'
  pg.defaults.port = 1234
  pg.defaults.host = 'blam'

  var client = new Client()
  assert.same(client, {
    user: 'boom',
    password: 'zap',
    database: 'pow',
    port: 1234,
    host: 'blam',
  })
})

suite.test('database defaults to user when user is non-default', () => {
  {
    pg.defaults.database = undefined

    const client = new Client({
      user: 'foo',
    })

    assert.strictEqual(client.database, 'foo')
  }

  {
    pg.defaults.database = 'bar'

    const client = new Client({
      user: 'foo',
    })

    assert.strictEqual(client.database, 'bar')
  }
})

suite.test('cleanup', () => {
  // restore process.env
  for (var key in realEnv) {
    process.env[key] = realEnv[key]
  }
})
