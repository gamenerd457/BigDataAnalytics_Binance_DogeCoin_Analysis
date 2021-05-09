 val dogeRdd=sc.textFile("dogecoin.csv")
 val dogeSchemaRdd=dogeRdd.map{ l=>
     val str0=l.split(";")
     val (dt,op,hg,lw,cl,vol,mkp)=(str0(0),str0(1),str0(2),str0(3),str0(4),str0(5),str0(6))
     (dt,op,hg,lw,cl,vol,mkp)}



  // dumping and plotting the high and low with respect to each date
 val dtReducedRdd1=dogeSchemaRdd.map{ case (dt,op,hg,lw,cl,vol,mkp) =>
      (dt,List((hg,lw)))}.reduceByKey(_ ++ _)
 dtReducedRdd1.toDF("Date","High,Low").show()
val doge_high_low_list=dtReducedRdd1.sortBy(_._1)
doge_high_low_list.toDF.show()
var file = new java.io.PrintStream("doge_high_low.csv")
dtReducedRdd1.collect.foreach{file.println(_)}
file.close()

// finding the largest recorded values in high
val doge_high=dogeSchemaRdd.sortBy(_._3,ascending=false).map{_._3.toFloat}
doge_high.toDF.show()
doge_high.toDF.show(1)

//finding the lowest recorded value in low
val doge_low=dogeSchemaRdd.sortBy(_._4).map{_._4.toFloat}
doge_low.toDF.show()
doge_low.toDF.show(1)



//finding the highest volume and also finding the respective date
val doge_vol=dogeSchemaRdd.sortBy(_._6,ascending=false).map{case (dt,op,hg,lw,cl,vol,mkp) => (vol,dt)}
doge_vol.toDF.show()

doge_vol.lookup("998929")(0)

//finding the largest market cap value and also the date
val doge_mkp=dogeSchemaRdd.map{case (dt,op,hg,lw,cl,vol,mkp) => (mkp.toInt,dt)}
 doge_mkp.sortBy(_._1,ascending=false).toDF.show()
 doge_mkp.lookup(423202000)(0)


//dumping and plotting high vs volume to find the relationship between them
val volReducedRdd=dogeSchemaRdd.map{ case (dt,op,hg,lw,cl,vol,mkp) =>
      (hg,vol)}
volReducedRdd.toDF("High","Volume").show()
var file = new java.io.PrintStream("doge_vol.csv")
volReducedRdd.collect.foreach{file.println(_)}
file.close()




//finding the date at which we have the highest value

val doge_dt_high=dogeSchemaRdd.map{case (dt,op,hg,lw,cl,vol,mkp) => (dt,hg) }
val doge_dt_high_sorted=doge_dt_high.sortBy(_._2,ascending=false)
doge_dt_high_sorted.toDF.show()


//finding the date at which we have the lowest value
val doge_dt_low=dogeSchemaRdd.map{case (dt,op,hg,lw,cl,vol,mkp) => (dt,lw) }
val doge_dt_low_sorted=doge_dt_low.sortBy(_._2)
doge_dt_low_sorted.toDF.show()


// finding the avg high
 val avg_high=dogeSchemaRdd.map{_._3.toFloat}.reduce(_+_)
 val doge_avg_high=avg_high/dogeSchemaRdd.map{_._3}.count

// finding the avg low
val avg_low=dogeSchemaRdd.map{_._4.toFloat}.reduce(_+_)
val doge_avg_low=avg_low/dogeSchemaRdd.map{_._4}.count

// finding the avg open
val avg_open=dogeSchemaRdd.map{_._2.toFloat}.reduce(_+_)
val doge_avg_open=avg_open/dogeSchemaRdd.map{_._2}.count

// finding the avg close
val avg_close=dogeSchemaRdd.map{_._5.toFloat}.reduce(_+_)
val doge_avg_close=avg_close/dogeSchemaRdd.map{_._5}.count

//finding the avg volume
val avg_volume=dogeSchemaRdd.map{_._6.toLong}.reduce(_+_)
val doge_avg_volume=avg_volume/dogeSchemaRdd.map{_._6}.count

//finding the avg market cap
val avg_mkp=dogeSchemaRdd.map{_._7.toLong}.reduce(_+_)
val doge_avg_mkp=avg_mkp/dogeSchemaRdd.map{_._7}.count


//dumping and plotting open and close with respect to each date
val dtReducedRdd2=dogeSchemaRdd.map{ case (dt,op,hg,lw,cl,vol,mkp) =>
      (dt,List((op,cl)))}.reduceByKey(_ ++ _)

var file = new java.io.PrintStream("doge_open_close.csv")

dtReducedRdd2.collect.foreach{file.println(_)}

file.close()


