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
