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
