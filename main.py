def main():
    print("Hello from coincall-rfq-maker!")

    # PRICING ENGINE
    """
    Refresh all pricing on
    - Every 0.1% move, or 
    - 2 minutes elapsed

    Refresh instrument pricing on
    - new leg added

    Min option price = $0.01
    """

    # RFQ MANAGER
    """
    On every new RFQ, add to RFQManager Object
    - Add new legs to PricingEngine and initialise pricing
    - Get existing legs' prices
    - calculate RFQ structure price (combination of all leg prices)
    - Submit quote to exchange and add quoteId to Quote Object

    On every price change in the PricingEngine
    - check if prices changed for each RFQ
    - if price changed, cancel existing quote and create new quote

    When RFQ is cancelled or expired
    - Remove RFQ from RFQManager Object
    - compare existing RFQ legs with the availabe legs in PricingEngine
    - legs which no longer exist in all RFQs to be removed from PricingEngine 
    """


if __name__ == "__main__":
    main()
