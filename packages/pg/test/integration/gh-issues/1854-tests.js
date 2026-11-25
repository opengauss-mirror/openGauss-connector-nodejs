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

const suite = new helper.Suite()

suite.test('Parameter serialization errors should not cause query to hang', (done) => {
  if (helper.config.native) {
    // pg-native cannot handle non-string parameters so skip this entirely
    return done()
  }
  const client = new helper.pg.Client()
  const expectedErr = new Error('Serialization error')
  client
    .connect()
    .then(() => {
      const obj = {
        toPostgres: function () {
          throw expectedErr
        },
      }
      return client.query('SELECT $1::text', [obj]).then(() => {
        throw new Error('Expected a serialization error to be thrown but no error was thrown')
      })
    })
    .catch((err) => {
      client.end(() => {})
      if (err !== expectedErr) {
        done(new Error('Expected a serialization error to be thrown but instead caught: ' + err))
        return
      }
      done()
    })
})
