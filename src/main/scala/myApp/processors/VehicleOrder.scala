package myApp.processors

case class VehicleOrder() extends OrderTrait() {
  override def orderRevenue(): Double = super.orderRevenue()

  override def orderTax(): Double = super.orderTax()
}
