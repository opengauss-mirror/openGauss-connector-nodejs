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
