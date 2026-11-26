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
var helper = require('./../test-helper')
var assert = require('assert')

const suite = new helper.Suite()

// allow skipping of this test via env var for
// local testing when you don't have SSL set up
if (process.env.PGTESTNOSSL) {
  return
}

suite.testAsync('it should connect over ssl', async () => {
  const ssl = helper.args.native
    ? 'require'
    : {
        rejectUnauthorized: false,
      }
  const client = new helper.pg.Client({ ssl })
  await client.connect()
  const { rows } = await client.query('SELECT NOW()')
  assert.strictEqual(rows.length, 1)
  await client.end()
})

suite.testAsync('it should fail with self-signed cert error w/o rejectUnauthorized being passed', async () => {
  const ssl = helper.args.native ? 'verify-ca' : {}
  const client = new helper.pg.Client({ ssl })
  try {
    await client.connect()
  } catch (e) {
    return
  }
  throw new Error('this test should have thrown an error due to self-signed cert')
})
