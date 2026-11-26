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

const suite = new helper.Suite()

suite.testAsync('BoundPool can be subclassed', async () => {
  const Pool = helper.pg.Pool
  class SubPool extends Pool {}
  const subPool = new SubPool()
  const client = await subPool.connect()
  client.release()
  await subPool.end()
  assert(subPool instanceof helper.pg.Pool)
})

suite.test('calling pg.Pool without new throws', () => {
  const Pool = helper.pg.Pool
  assert.throws(() => {
    const pool = Pool()
  })
})
