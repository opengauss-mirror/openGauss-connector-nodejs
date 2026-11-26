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

const pg = require('../../../lib')
const helper = require('../test-helper')
const suite = new helper.Suite()

suite.testAsync('timeout causing query crashes', async () => {
  const client = new helper.Client()
  await client.connect()
  await client.query('CREATE TEMP TABLE foobar( name TEXT NOT NULL, id SERIAL)')
  await client.query('BEGIN')
  await client.query("SET LOCAL statement_timeout TO '1ms'")
  let count = 0
  while (count++ < 5000) {
    try {
      await client.query('INSERT INTO foobar(name) VALUES ($1)', [Math.random() * 1000 + ''])
    } catch (e) {
      await client.query('ROLLBACK')
    }
  }
  await client.end()
})
