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
const helper = require('./../test-helper')
const assert = require('assert')
const util = require('util')

const suite = new helper.Suite()

const secret_value = 'FAIL THIS TEST'

suite.test('SSL Key should not exist in toString() output', () => {
  const pool = new helper.pg.Pool({ ssl: { key: secret_value } })
  const client = new helper.pg.Client({ ssl: { key: secret_value } })
  assert(pool.toString().indexOf(secret_value) === -1)
  assert(client.toString().indexOf(secret_value) === -1)
})

suite.test('SSL Key should not exist in util.inspect output', () => {
  const pool = new helper.pg.Pool({ ssl: { key: secret_value } })
  const client = new helper.pg.Client({ ssl: { key: secret_value } })
  const depth = 20
  assert(util.inspect(pool, { depth }).indexOf(secret_value) === -1)
  assert(util.inspect(client, { depth }).indexOf(secret_value) === -1)
})

suite.test('SSL Key should not exist in json.stringfy output', () => {
  const pool = new helper.pg.Pool({ ssl: { key: secret_value } })
  const client = new helper.pg.Client({ ssl: { key: secret_value } })
  const depth = 20
  assert(JSON.stringify(pool).indexOf(secret_value) === -1)
  assert(JSON.stringify(client).indexOf(secret_value) === -1)
})

suite.test('SSL Key should exist for direct access', () => {
  const pool = new helper.pg.Pool({ ssl: { key: secret_value } })
  const client = new helper.pg.Client({ ssl: { key: secret_value } })
  assert(pool.options.ssl.key === secret_value)
  assert(client.connectionParameters.ssl.key === secret_value)
})

suite.test('SSL Key should exist for direct access even when non-enumerable custom config', () => {
  const config = { ssl: { key: secret_value } }
  Object.defineProperty(config.ssl, 'key', { enumerable: false })
  const pool = new helper.pg.Pool(config)
  const client = new helper.pg.Client(config)
  assert(pool.options.ssl.key === secret_value)
  assert(client.connectionParameters.ssl.key === secret_value)
})
