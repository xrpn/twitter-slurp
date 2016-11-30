package com.xrpn.jhi.json

import com.twitter.Extractor
import com.vdurmont.emoji.EmojiParser
import twitter4j.JSONObjectType.Type._
import twitter4j.{JSONObjectType, JSONTokener, Status, T4JJsonImplGremlin, JSONObject => T4JJSONObject}

import scala.collection.JavaConverters._
import scala.util.matching.Regex.MatchIterator


/**
  * JsonWrangler is a collection of utilities for data extraction.  In a more OO setting
  * this could have been an object constructed with the json message as argument.  This
  * implementation is leaner, in a functional sense.
  * Created by alsq on 11/19/16.
  */
object JsonWrangler {

  val statusIdentifierField = "text"
  val simpleEmojiAliasRegex = """(:.*?:)""".r

  /**
    * Check for pictures, restricted to the requirements of this POC.
    * @param someUrl some url in the Tweet
    * @return true if the url meets the "is a picture" requirement
    */
  private def hasPicKeywords(someUrl: String): Boolean = {
    someUrl.toLowerCase.contains("instagram") || someUrl.toLowerCase.contains("pic.twitter.com")
  }

  /**
    * Twitter4j object based representation of a Tweet.  Take some raw json,
    * and if it looks like a tweet then parse it at make it into an object.  The
    * return type could be an Option, but a List works better in for comprehension.
    * @param json a raw json message, not necessarily a tweet.
    * @return Twitter4j representation of a tweet, "Status", as a list with one
    *         element if the raw json is a tweet, or an empty list otherwise.
    */
  def raw2statusObj(json: String): List[Status] = {
    val job = new T4JJSONObject(json)
    JSONObjectType.determine(job) match {
      case STATUS => List(T4JJsonImplGremlin.getStatus(job))
      case _ => Nil
    }
  }

  /**
    * Twitter4j extraction of the representation of a Tweet as Json.  Take some
    * raw json, and if it looks like a tweet then return it.  The return type
    * could be an Option, but a List works better in for comprehension.
    * @param json a raw json message, not necessarily a tweet.
    * @return Twitter4j extraction of the raw json, as a list with one
    *         element if the raw json is a tweet, or an empty list otherwise.
    */
  def raw2statusStr(json: String): List[String] = {
    val job = new T4JJSONObject(new JSONTokener(json))
    JSONObjectType.determine(job) match {
      case STATUS => List(job.get("text").toString)
      case _ => Nil
    }
  }

  /**
    * The hashtags in the tweet, according to Twitter4j.
    * @param status Twitter4j object based representation of a Tweet
    * @return a list with all the hashtags in the tweet, if any, or
    *         an empty list otherwise
    */
  def statusObj2hashtags(status: Status): List[String] = {
    (for {
      hte <-status.getHashtagEntities
      if hte.getText.nonEmpty
    } yield hte.getText).toList
  }

  /**
    * The hashtags in the tweet, according to com.twitter.Extractor
    * @param status json based representation of a Tweet
    * @return a list with all the hashtags in the tweet, if any, or
    *         an empty list otherwise
    */
  def statusStr2hashtags(status: String): List[String] = {
      new Extractor().extractHashtags(status).asScala.toList.filterNot(_.isEmpty)
  }

  /**
    * The urls in the tweet.
    * @param status Twitter4j object based representation of a Tweet
    * @return a list with all the urls in the tweet, if any, or
    *         an empty list otherwise
    */
  def statusObj2url(status: Status, isXpn: Boolean = true)(implicit debug: Boolean): List[String] = {
    val sUrl = (for {
      ue <-status.getURLEntities
      if ue.getURL.nonEmpty
    } yield ue.getURL).toList

    if (debug) println(s"sUrl:${sUrl.mkString(" | ")}")

    // TODO bad hack warning, this likely is a buggy workaround but it works for now
    // Apparently there is a disagreement btw. twitter4j and com.twitter.Extractor
    // in regard to what needs to go in the url field of status; this is a workaround
    // for what looks like be a bug in twitter4j, which may or may not be as I'm not
    // a Twitter expert and my opinion of the operation of the spritzer is shallow.
    // Anyway, if com.twitter.Extractor is the reference implementation, which I doubt,
    // and if it is correct, which I also doubt, enforcing agreement is a possible way to
    // do a bit better than trivial testing.  In fact, it appears com.twitter.Extractor
    // fails to parse correctly certain url forms (e.g. truncated); hmm.
    // TODO the real solution is to fix the library, once better understanding available

    val mUrl = (for {
      me <-status.getMediaEntities
      if me.getURL.nonEmpty
    } yield if (isXpn && me.getExpandedURL.nonEmpty) me.getExpandedURL else me.getURL).toList

    if (debug) println(s"mUrl:${mUrl.mkString(" | ")}")

    val xmUrl = (for {
      xme <-status.getMediaEntities
      if xme.getURL.nonEmpty
    } yield if (isXpn && xme.getExpandedURL.nonEmpty) xme.getExpandedURL else xme.getURL).toList

    if (debug) println(s"xmUrl:${xmUrl.mkString(" | ")}")

    // In case of retweet, recursively check.  Sigh.  T4J may be broken,
    // but com.twitter may also be broken in a different way.
    val optRetweet = Option(status.getRetweetedStatus)
    val rtwUrl = optRetweet.map(rtw => statusObj2url(rtw,isXpn)).getOrElse(Nil)

    if (debug) println(s"rtwUrl:${rtwUrl.mkString(" | ")}")

    (sUrl ::: mUrl ::: xmUrl ::: rtwUrl).distinct
  }

  /**
    * The urls in the tweet, according to com.twitter.Extractor.
    * @param status json based representation of a Tweet
    * @return a list with all the urls in the tweet, if any, or
    *         an empty list otherwise
    */
  def statusStr2url(status: String): List[String] = {
    new Extractor().extractURLs(status).asScala.toList.filterNot(_.isEmpty)
  }

  /**
    * The photo-urls in the tweet.  A photo-url is a url that meets the
    * requirements of being a photo-url, as established for this POC.
    * @param status Twitter4j object based representation of a Tweet
    * @return a list with all the urls in the tweet, if any, or
    *         an empty list otherwise
    */
  def statusObj2photoUrl(status: Status)(implicit debug: Boolean): List[String] = {

    val mPurl = (for {
      me <-status.getMediaEntities
      if me.getDisplayURL.nonEmpty
      if hasPicKeywords(me.getDisplayURL)
    } yield me.getDisplayURL).toList

    if (debug) println(s"mPurl:${mPurl.mkString(" | ")}")

    val xmPurl = (for {
      xme <-status.getExtendedMediaEntities
      if xme.getDisplayURL.nonEmpty
      if hasPicKeywords(xme.getDisplayURL)
    } yield xme.getDisplayURL).toList

    if (debug) println(s"xmPurl:${xmPurl.mkString(" | ")}")

    val optRetweet = Option(status.getRetweetedStatus)
    val rtwPurl = optRetweet.map(rtw => statusObj2photoUrl(rtw)).getOrElse(Nil)

    if (debug) println(s"rtwPurl:${rtwPurl.mkString(" | ")}")

    (mPurl ::: xmPurl ::: rtwPurl).distinct
  }

  /**
    * The emojis in the tweet.  Emojis are reported as "aliases", where
    * :smile: means a happy face.  Makes for a more readable result on a
    * terminal when using $curl, or for troubleshooting.
    * @param status Twitter4j object based representation of a Tweet
    * @return a list with all the urls in the tweet, if any, or
    *         an empty list otherwise
    */
  def statusObj2emoji(status: Status): List[String] = {
    val tweet = status.getText
    // remove ':' not to interfere later with alias extraction
    val mangled = if (!tweet.contains(":")) tweet else tweet.replace(':','=')
    val tweetWithAlias = EmojiParser.parseToAliases(mangled)
    // here there should be no ':' which are not part of an alias
    val aliases: MatchIterator = simpleEmojiAliasRegex.findAllIn(tweetWithAlias)
    aliases.toList
  }

  /**
    * Extract the hashtags from json, if the json is a tweet.
    * @param json a twitter message, not necessarily a tweet.
    * @return if json is indeed a tweet, and the tweet contains one
    *         or more hashtags, then a list of them.  Otherwise,
    *         an empty list.
    */
  def raw2hashtags(json: String): List[String] = {
    for {
      status <- raw2statusObj(json)
      ht <- statusObj2hashtags(status)
      if ht.nonEmpty
    } yield ht
  }

  /**
    * Extract the urls from json, if the json is a tweet.
    * @param json a twitter message, not necessarily a tweet.
    * @param debug pretend this does not exist; if true, it will
    *              emit debug info to stdout
    * @return if json is indeed a tweet, and the tweet contains one
    *         or more urls, then a list of them.  Otherwise,
    *         an empty list.
    */
  def raw2url(json: String)(implicit debug: Boolean): List[String] = {
    for {
      status <- raw2statusObj(json)
      url <- statusObj2url(status)
      if url.nonEmpty
    } yield url
  }

  /**
    * Extract the photo-urls from json, if the json is a tweet.
    * @param json a twitter message, not necessarily a tweet.
    * @param debug pretend this does not exist; if true, it will
    *              emit debug info to stdout
    * @return if json is indeed a tweet, and the tweet contains one
    *         or more photo-urls, then a list of them.  Otherwise,
    *         an empty list.
    */
  def raw2photoUrl(json: String)(implicit debug: Boolean): List[String] = {
    for {
      status <- raw2statusObj(json)
      url <- statusObj2photoUrl(status)
      if url.nonEmpty
    } yield url
  }

  /**
    * Extract the emoji from json, if the json is a tweet.
    * @param json a twitter message, not necessarily a tweet.
    * @return if json is indeed a tweet, and the tweet contains one
    *         or more emoji, then a list of their aliases.  Otherwise,
    *         an empty list.
    */
  def raw2emoji(json: String): List[String] = {
    raw2statusObj(json).flatMap(statusObj2emoji)
  }
}
