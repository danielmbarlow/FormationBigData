package myApp.processors

case class Order(
                invoice : BillingSchema,
                product : BillingDetail
                )  {

  def computeRevenue(quantity : Int, unitPrice: Double) : Double = {
    return quantity * unitPrice
  }

  def computeTaxes(taxRate: Double, billingAmount: Double): Double = {
    return (taxRate * billingAmount).round
  }
}
