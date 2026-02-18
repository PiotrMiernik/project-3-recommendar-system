// Description: Creates database, collection, and tests inserting Amazon reviews

// 1. Switch to the 'recommender' database. It will be created if it doesn't exist.
use("recommender");

// 2. Optional: Drop the collection if running the script multiple times to ensure a clean state.
db.reviews_raw.drop();

// 3. Insert the sample document
db.reviews_raw.insertOne({
  "overall": 2.0,
  "vote": "12",
  "verified": false,
  "reviewTime": "09 22, 2016",
  "reviewerID": "A1IDMI31WEANAF",
  "asin": "0020232233",
  "reviewerName": "Mackenzie Kent",
  "reviewText": "When it comes to a DM's screen, the space on the screen itself is at an absolute premium. The fact that 50% of this space is wasted on art (and not terribly informative or needed art as well) makes it completely useless. The only reason that I gave it 2 stars and not 1 was that, technically speaking, it can at least still stand up to block your notes and dice rolls. Other than that, it drops the ball completely.",
  "summary": "The fact that 50% of this space is wasted on art (and not terribly informative or needed art ...",
  "unixReviewTime": 1474502400
});

// 4. Verify the data was inserted correctly (find and count)
print("--- Data Verification ---");

const count = db.reviews_raw.countDocuments();
print("Number of documents in collection: " + count);

// Display the first document to verify the format
const doc = db.reviews_raw.findOne();
printjson(doc);