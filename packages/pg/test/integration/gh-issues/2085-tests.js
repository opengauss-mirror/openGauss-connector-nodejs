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
