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

const password = 'FAIL THIS TEST'

suite.test('Password should not exist in toString() output', () => {
  const pool = new helper.pg.Pool({ password })
  const client = new helper.pg.Client({ password })
  assert(pool.toString().indexOf(password) === -1)
  assert(client.toString().indexOf(password) === -1)
})

suite.test('Password should not exist in util.inspect output', () => {
  const pool = new helper.pg.Pool({ password })
  const client = new helper.pg.Client({ password })
  const depth = 20
  assert(util.inspect(pool, { depth }).indexOf(password) === -1)
  assert(util.inspect(client, { depth }).indexOf(password) === -1)
})

suite.test('Password should not exist in json.stringfy output', () => {
  const pool = new helper.pg.Pool({ password })
  const client = new helper.pg.Client({ password })
  const depth = 20
  assert(JSON.stringify(pool).indexOf(password) === -1)
  assert(JSON.stringify(client).indexOf(password) === -1)
})
