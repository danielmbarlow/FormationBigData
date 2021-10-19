import myApp.processors.{BillingDetail, BillingSchema, Order}

object ObjectOriented {
  def main(args: Array[String]) : Unit = {
    val invoice = BillingSchema("BK34", "20/10/2021", "Daniel", 3500)
    val detail = BillingDetail(invoice, "CB28", 1, 1.20)
    println(invoice)

    val order = Order(invoice, detail)

    println(s"Revenue: ${order.computeRevenue(detail.quantity, detail.unitPrice)}")
    println(s"Taxes: ${order.computeTaxes(17.5, 10)}")
  }
}
