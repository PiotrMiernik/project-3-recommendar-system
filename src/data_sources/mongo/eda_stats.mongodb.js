use("recommender");

//EDA - GLOBAL DATASET OVERVIEW - Focused on general statistics and quality checks.


// Configuration
const DAYS = 365;
const NOW_EPOCH = Math.floor(Date.now() / 1000);
const CUTOFF_EPOCH = NOW_EPOCH - DAYS * 24 * 60 * 60;

// Global rating stats (Overall health of the dataset)
console.log("--- GENERAL RATING STATISTICS (FULL DATASET) ---");
db.reviews_raw.aggregate([
  { $match: { overall: { $type: "number" } } },
  {
    $group: {
      _id: null,
      avg_rating: { $avg: "$overall" },
      min_rating: { $min: "$overall" },
      max_rating: { $max: "$overall" },
      total_reviews: { $sum: 1 }
    }
  }
]).pretty();


// Rating distribution (Check for data imbalance)
console.log("--- RATING DISTRIBUTION (FULL DATASET) ---");
db.reviews_raw.aggregate([
  { $match: { overall: { $type: "number" } } },
  { $group: { _id: "$overall", count: { $sum: 1 } } },
  { $sort: { _id: -1 } }
]).pretty();


// Average review text length (Useful for Embedding strategy in Spark)
console.log("--- AVERAGE REVIEW LENGTH (FULL DATASET) ---");
db.reviews_raw.aggregate([
  {
    $project: {
      textLength: { $strLenCP: { $ifNull: ["$reviewText", ""] } }
    }
  },
  {
    $group: {
      _id: null,
      avg_length: { $avg: "$textLength" },
      max_length: { $max: "$textLength" }
    }
  }
]).pretty();


// Data Freshness Check (Unix Time Analysis)
console.log("--- DATA FRESHNESS (RECENT VS OLD) ---");
db.reviews_raw.aggregate([
  {
    $group: {
      _id: null,
      newest_review: { $max: "$unixReviewTime" },
      oldest_review: { $min: "$unixReviewTime" },
      reviews_in_window: {
        $sum: { $cond: [{ $gte: ["$unixReviewTime", CUTOFF_EPOCH] }, 1, 0] }
      }
    }
  },
  {
    $project: {
      _id: 0,
      newest: { $toDate: { $multiply: ["$newest_review", 1000] } },
      oldest: { $toDate: { $multiply: ["$oldest_review", 1000] } },
      reviews_in_last_year: "$reviews_in_window"
    }
  }
]).pretty();


// MATERIALIZATION SECTION (Optional) - Use this if you want to save global stats for further analysis

/*
db.reviews_raw.aggregate([
  { $match: { overall: { $type: "number" } } },
  {
    $group: {
      _id: "global_stats",
      avg_rating: { $avg: "$overall" },
      total_reviews: { $sum: 1 },
      last_updated: { $sum: NOW_EPOCH }
    }
  },
  {
    $merge: {
      into: "eda_summary",
      on: "_id",
      whenMatched: "replace",
      whenNotMatched: "insert"
    }
  }
]);
*/