/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * All rights reserved.
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/license/mit
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
