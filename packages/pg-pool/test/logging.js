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

const expect = require('expect.js')

const describe = require('mocha').describe
const it = require('mocha').it

const Pool = require('../')

describe('logging', function () {
  it('logs to supplied log function if given', function () {
    const messages = []
    const log = function (msg) {
      messages.push(msg)
    }
    const pool = new Pool({ log: log })
    return pool.query('SELECT NOW()').then(function () {
      expect(messages.length).to.be.greaterThan(0)
      return pool.end()
    })
  })
})
