package utils

import java.io.UnsupportedEncodingException
import java.util


/**
  * Description: 
  *
  * @author YKL on 2018/6/6.
  * @version 1.0
  *          spark:梦想开始的地方
  */
object CosineUtil {

  /**
    * 判断是否是汉字
    *
    * @param ch
    * @return
    */
  def isHanZi(ch: Char): Boolean = ch >= 0x4E00 && ch <= 0x9FA5

  /**
    * 根据输入的Unicode字符，获取它的GB2312编码或者ascii编码，
    *
    * @param ch 输入的GB2312中文字符或者ASCII字符(128个)
    * @return ch在GB2312中的位置，-1表示该字符不认识
    */
  def getGB2312ID(ch: Char): Short = {
    try {
      val buffer = Character.toString(ch).getBytes("GB2312")
      if (buffer.length != 2) {
        /** 正常情况下buffer应该是两个字节，否则说明ch不属于GB2312编码，故返回'?'，此时说明不认识该字符 **/
        return -1
      }
      val b0 = (buffer(0) & 0x0FF) - 161
      /** 编码从A1开始，因此减去0xA1=161 **/
      val b1 = (buffer(1) & 0x0FF) - 161
      // 第一个字符和最后一个字符没有汉字，因此每个区只收16*6-2=94个汉字
      return (b0 * 94 + b1).toShort
    }
    catch {
      case e: UnsupportedEncodingException =>
        e.printStackTrace()
    }
    -1
  }

  def getSimilarity(doc1: String, doc2: String): Double = {
    if (doc1 != null && doc1.trim().length() > 0 && doc2 != null && doc2.trim().length() > 0) {
      var algorithmMap = new util.HashMap[Int, Array[Int]]();

      //将两个字符串中的中文字符以及出现的总数封装到，AlgorithmMap中
      for (i <- 0 until doc1.length) {
        val d1 = doc1.charAt(i)
        if (isHanZi(d1)) {
          //标点和数字不处理
          val charIndex = getGB2312ID(d1).toInt //保存字符对应的GB2312编码
          if (charIndex != -1) {
            var fq = algorithmMap.get(charIndex)
            if (fq != null && fq.length == 2) {
              fq(0) = fq(0) + 1; //已有该字符，加1
            } else {
              fq = new Array[Int](2)
              fq(0) = 1
              fq(1) = 0
              algorithmMap.put(charIndex, fq)//新增字符入map
            }
          }
        }
      }

      for (i <- 0 until doc2.length) {
        val d2 = doc2.charAt(i)
        if (isHanZi(d2)) {
          //标点和数字不处理
          val charIndex = getGB2312ID(d2).toInt //保存字符对应的GB2312编码
          if (charIndex != -1) {
            var fq = algorithmMap.get(charIndex)
            if (fq != null && fq.length == 2) {
              fq(1) = fq(1) + 1; //已有该字符，加1
            } else {
              fq = new Array[Int](2)
              fq(0) = 0
              fq(1) = 1
              algorithmMap.put(charIndex, fq) //新增字符入map
            }
          }
        }
      }

      val iterator = algorithmMap.keySet.iterator
      var sqdoc1 = 0
      var sqdoc2 = 0
      var denominator = 0
      while (iterator.hasNext) {
        val c = algorithmMap.get(iterator.next())
        denominator += c(0) * c(1)
        sqdoc1 += c(0) * c(0)
        sqdoc2 += c(1) * c(1)
      }

      return denominator / Math.sqrt(sqdoc1 * sqdoc2); //余弦计算
    } else {
      throw new NullPointerException(" the Document is null or have not chars!!");
    }
  }

}
