use('recommender');

/*
INDEX 1: Product + Time
Supports product-level feature engineering:
- Reviews per product
- Product rating trends over time
- Incremental reads per product (sorted by most recent)
*/
db.reviews_raw.createIndex(
  { asin: 1, unixReviewTime: -1 }
);


/*
INDEX 2: User + Time
Supports user-level feature engineering:
- User activity over time
- User rating behavior analysis
- Time-based user aggregates
*/
db.reviews_raw.createIndex(
  { reviewerID: 1, unixReviewTime: -1 }
);


/*
INDEX 3: Rating
Supports rating distribution analysis and quality filtering:
- Rating distribution (EDA)
- Filters like overall >= 4
*/
db.reviews_raw.createIndex(
  { overall: 1 }
);



