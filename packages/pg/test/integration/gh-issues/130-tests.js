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
var helper = require('../test-helper')
var exec = require('child_process').exec

helper.pg.defaults.poolIdleTimeout = 1000

const pool = new helper.pg.Pool()
pool.connect(function (err, client, done) {
  assert.ifError(err)
  client.once('error', function (err) {
    client.on('error', (err) => {})
    done(err)
  })
  client.query('SELECT pg_backend_pid()', function (err, result) {
    assert.ifError(err)
    var pid = result.rows[0].pg_backend_pid
    var psql = 'psql'
    if (helper.args.host) psql = psql + ' -h ' + helper.args.host
    if (helper.args.port) psql = psql + ' -p ' + helper.args.port
    if (helper.args.user) psql = psql + ' -U ' + helper.args.user
    exec(
      psql + ' -c "select pg_terminate_backend(' + pid + ')" template1',
      assert.calls(function (error, stdout, stderr) {
        assert.ifError(error)
      })
    )
  })
})
