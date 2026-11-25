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
const co = require('co')
const expect = require('expect.js')

const describe = require('mocha').describe
const it = require('mocha').it
const BluebirdPromise = require('bluebird')

const Pool = require('../')

const checkType = (promise) => {
  expect(promise).to.be.a(BluebirdPromise)
  return promise.catch((e) => undefined)
}

describe('Bring your own promise', function () {
  it(
    'uses supplied promise for operations',
    co.wrap(function* () {
      const pool = new Pool({ Promise: BluebirdPromise })
      const client1 = yield checkType(pool.connect())
      client1.release()
      yield checkType(pool.query('SELECT NOW()'))
      const client2 = yield checkType(pool.connect())
      // TODO - make sure pg supports BYOP as well
      client2.release()
      yield checkType(pool.end())
    })
  )

  it(
    'uses promises in errors',
    co.wrap(function* () {
      const pool = new Pool({ Promise: BluebirdPromise, port: 48484 })
      yield checkType(pool.connect())
      yield checkType(pool.end())
      yield checkType(pool.connect())
      yield checkType(pool.query())
      yield checkType(pool.end())
    })
  )
})
