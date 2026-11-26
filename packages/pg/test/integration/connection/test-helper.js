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
var net = require('net')
var helper = require('../test-helper')
var Connection = require('../../../lib/connection')
var utils = require('../../../lib/utils')
var connect = function (callback) {
  var username = helper.args.user
  var database = helper.args.database
  var con = new Connection({ stream: new net.Stream() })
  con.on('error', function (error) {
    console.log(error)
    throw new Error('Connection error')
  })
  con.connect(helper.args.port || '5432', helper.args.host || 'localhost')
  con.once('connect', function () {
    con.startup({
      user: username,
      database: database,
    })
    con.once('authenticationCleartextPassword', function () {
      con.password(helper.args.password)
    })
    con.once('authenticationMD5Password', function (msg) {
      con.password(utils.postgresMd5PasswordHash(helper.args.user, helper.args.password, msg.salt))
    })
    con.once('readyForQuery', function () {
      con.query('create temp table ids(id integer)')
      con.once('readyForQuery', function () {
        con.query('insert into ids(id) values(1); insert into ids(id) values(2);')
        con.once('readyForQuery', function () {
          callback(con)
        })
      })
    })
  })
}

module.exports = {
  connect: connect,
}
