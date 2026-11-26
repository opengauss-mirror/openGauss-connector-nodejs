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

const assert = require('assert')
const helper = require('../test-helper')
const suite = new helper.Suite()
const { Client } = helper.pg

suite.test('it sends options', async () => {
  const client = new Client({
    options: '--default_transaction_isolation=serializable',
  })
  await client.connect()
  const { rows } = await client.query('SHOW default_transaction_isolation')
  assert.strictEqual(rows.length, 1)
  assert.strictEqual(rows[0].default_transaction_isolation, 'serializable')
  await client.end()
})
