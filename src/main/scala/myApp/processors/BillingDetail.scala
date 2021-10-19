package myApp.processors

case class BillingDetail(
                          billingSchema: BillingSchema,
                          productId: String,
                          quantity: Int,
                          unitPrice: Double
                        )
