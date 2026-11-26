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
const expect = require('expect.js')

const describe = require('mocha').describe
const it = require('mocha').it

const Pool = require('../')

describe('verify', () => {
  it('verifies a client with a callback', (done) => {
    const pool = new Pool({
      verify: (client, cb) => {
        cb(new Error('nope'))
      },
    })

    pool.connect((err, client) => {
      expect(err).to.be.an(Error)
      expect(err.message).to.be('nope')
      pool.end()
      done()
    })
  })
})
