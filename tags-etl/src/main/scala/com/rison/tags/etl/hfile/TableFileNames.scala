package com.rison.tags.etl.hfile

import scala.collection.immutable.TreeMap

/**
 * @author : Rison 2021/5/21 上午10:07
 *         HBase 中各个表中的字段名称，存储到TreeMap中
 */
object TableFileNames {

  //TODO : 使用TreeMap为qualifier做字典排序

  //a. 行为日志数据表字段
  val LOG_FIELD_NAMES: TreeMap[String, Int] = TreeMap(
    ("id", 0),
    ("log_id", 1),
    ("remote_ip", 2),
    ("site_global_ticket", 3),
    ("site_global_session", 4),
    ("global_user_id", 5),
    ("cookie_text", 6),
    ("user_agent", 7),
    ("ref_url", 8),
    ("loc_url", 9),
    ("log_time", 10)
  )

  // b. 商品信息数据表的字段
  val GOODS_FIELD_NAMES: TreeMap[String, Int] = TreeMap(
    ("id", 0),
    ("siteid", 1),
    ("istest", 2),
    ("hasread", 3),
    ("supportonedaylimit", 4),
    ("orderid", 5),
    ("cordersn", 6),
    ("isbook", 7),
    ("cpaymentstatus", 8),
    ("cpaytime", 9),
    ("producttype", 10),
    ("productid", 11),
    ("productname", 12),
    ("sku", 13),
    ("price", 14),
    ("number", 15),
    ("lockednumber", 16),
    ("unlockednumber", 17),
    ("productamount", 18),
    ("balanceamount", 19),
    ("couponamount", 20),
    ("esamount", 21),
    ("giftcardnumberid", 22),
    ("usedgiftcardamount", 23),
    ("couponlogid", 24),
    ("activityprice", 25),
    ("activityid", 26),
    ("cateid", 27),
    ("brandid", 28),
    ("netpointid", 29),
    ("shippingfee", 30),
    ("settlementstatus", 31),
    ("receiptorrejecttime", 32),
    ("iswmssku", 33),
    ("scode", 34),
    ("tscode", 35),
    ("tsshippingtime", 36)
  )

  // c. 用户信息数据表的字段
  val USER_FIELD_NAMES: TreeMap[String, Int] = TreeMap(
    ("id", 0),
    ("siteid", 1),
    ("avatarimagefileid", 2),
    ("email", 3),
    ("username", 4),
    ("password", 5),
    ("salt", 6),
    ("registertime", 7),
    ("lastlogintime", 8),
    ("lastloginip", 9),
    ("memberrankid", 10),
    ("bigcustomerid", 11),
    ("lastaddressid", 12),
    ("lastpaymentcode", 13),
    ("gender", 14),
    ("birthday", 15),
    ("qq", 16),
    ("job", 17),
    ("mobile", 18),
    ("politicalface", 19),
    ("nationality", 20),
    ("validatecode", 21),
    ("pwderrcount", 22),
    ("source", 23),
    ("marriage", 24),
    ("money", 25),
    ("moneypwd", 26),
    ("isemailverify", 27),
    ("issmsverify", 28),
    ("smsverifycode", 29),
    ("emailverifycode", 30),
    ("verifysendcoupon", 31),
    ("canreceiveemail", 32),
    ("modified", 33),
    ("channelid", 34),
    ("grade_id", 35),
    ("nick_name", 36),
    ("is_blacklist", 37)
  )
  // d. 订单数据表的字段
  val ORDER_FIELD_NAMES: TreeMap[String, Int] = TreeMap(
    ("id", 0),
    ("siteid", 1),
    ("istest", 2),
    ("hassync", 3),
    ("isbackend", 4),
    ("isbook", 5),
    ("iscod", 6),
    ("notautoconfirm", 7),
    ("ispackage", 8),
    ("packageid", 9),
    ("ordersn", 10),
    ("relationordersn", 11),
    ("memberid", 12),
    ("predictid", 13),
    ("memberemail", 14),
    ("addtime", 15),
    ("synctime", 16),
    ("orderstatus", 17),
    ("paytime", 18),
    ("paymentstatus", 19),
    ("receiptconsignee", 20),
    ("receiptaddress", 21),
    ("receiptzipcode", 22),
    ("receiptmobile", 23),
    ("productamount", 24)
  )





}
