 val bnbRdd=sc.textFile("binance-coin.csv")
 val bnbSchemaRdd=bnbRdd.map{ l=>
     val str0=l.split(";")
     val (dt,op,hg,lw,cl,vol,mkp)=(str0(0),str0(1),str0(2),str0(3),str0(4),str0(5),str0(6))
     (dt,op,hg,lw,cl,vol,mkp)}




 // dumping and plotting the high and low with respect to each date
 val dtReducedRdd1=bnbSchemaRdd.map{ case (dt,op,hg,lw,cl,vol,mkp) =>
      (dt,List((hg,lw)))}.reduceByKey(_ ++ _)
 dtReducedRdd1.toDF("Date","High,Low").show()
val bnb_high_low_list=dtReducedRdd1.sortBy(_._1)
bnb_high_low_list.toDF.show()
var file = new java.io.PrintStream("bnb_high_low.csv")
dtReducedRdd1.collect.foreach{file.println(_)}
file.close()

// finding the largest recorded values in high
val bnb_high=bnbSchemaRdd.sortBy(_._3,ascending=false).map{_._3.toFloat}
bnb_high.toDF.show()
bnb_high.toDF.show(1)

// finding the lowest recorded value in low
val bnb_low=bnbSchemaRdd.sortBy(_._4).map{_._4.toFloat}
bnb_low.toDF.show()
bnb_low.toDF.show(1)



//finding the highest volume and also finding the respective date
val bnb_vol=bnbSchemaRdd.sortBy(_._6,ascending=false).map{case (dt,op,hg,lw,cl,vol,mkp) => (vol,dt)}
bnb_vol.lookup("11155000")(0)

//finding the largest market cap value and also the date

val bnb_mkp=bnbSchemaRdd.map{case (dt,op,hg,lw,cl,vol,mkp) => (mkp.toInt,dt)}
 bnb_mkp.sortBy(_._1,ascending=false).toDF.show()
 bnb_mkp.lookup(272445000)(0)




//dumping and plotting high vs volume to find the relationship between them
val volReducedRdd=bnbSchemaRdd.map{ case (dt,op,hg,lw,cl,vol,mkp) =>
      (hg,vol)}
volReducedRdd.toDF("High","Volume").show()
var file = new java.io.PrintStream("bnb_vol.csv")
volReducedRdd.collect.foreach{file.println(_)}
file.close()








//finding the date at which we have the highest value
val bnb_dt_high=bnbSchemaRdd.map{case (dt,op,hg,lw,cl,vol,mkp) => (dt,hg) }
val bnb_dt_high_sorted=bnb_dt_high.sortBy(_._2,ascending=false)
bnb_dt_high_sorted.toDF.show()

//finding the date at which we have the lowest value
val bnb_dt_low=bnbSchemaRdd.map{case (dt,op,hg,lw,cl,vol,mkp) => (dt,lw) }
val bnb_dt_low_sorted=bnb_dt_low.sortBy(_._2)
bnb_dt_low_sorted.toDF.show()

// finding the avg high
 val avg_high=bnbSchemaRdd.map{_._3.toFloat}.reduce(_+_)
 val bnb_avg_high=avg_high/bnbSchemaRdd.map{_._3}.count

//finding the avg low

val avg_low=bnbSchemaRdd.map{_._4.toFloat}.reduce(_+_)
val bnb_avg_low=avg_low/bnbSchemaRdd.map{_._4}.count

//finding the avg open 
val avg_open=bnbSchemaRdd.map{_._2.toFloat}.reduce(_+_)
val bnb_avg_open=avg_open/bnbSchemaRdd.map{_._2}.count

//finding the avg close
val avg_close=bnbSchemaRdd.map{_._5.toFloat}.reduce(_+_)
val bnb_avg_close=avg_close/bnbSchemaRdd.map{_._5}.count

//finding the avg volume
val avg_volume=bnbSchemaRdd.map{_._6.toInt}.reduce(_+_)
val bnb_avg_volume=avg_volume/bnbSchemaRdd.map{_._6}.count

//finding the avg marketcap
val avg_mkp=bnbSchemaRdd.map{_._7.toLong}.reduce(_+_)
val bnb_avg_mkp=avg_mkp/bnbSchemaRdd.map{_._7}.count

//dumping and plotting open and close with respect to each date
val dtReducedRdd2=bnbSchemaRdd.map{ case (dt,op,hg,lw,cl,vol,mkp) =>
      (dt,List((op,cl)))}.reduceByKey(_ ++ _)

var file = new java.io.PrintStream("bnb_open_close.csv")

dtReducedRdd2.collect.foreach{file.println(_)}

file.close()