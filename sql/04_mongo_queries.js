// PURPOSE: 5 MongoDB queries for user engagement analysis
// Run inside mongosh with:
// load("sql/04_mongo_queries.js")

// -------------------------------------------------------
// QUERY 1: All ad interactions for a specific user
// -------------------------------------------------------
print("\n=== QUERY 1: Ad interactions for user 59472 ===");
printjson(db.user_engagement.findOne(
    { user_id: 59472 },
    {
        user_id:   1,
        age:       1,
        gender:    1,
        location:  1,
        interests: 1,
        sessions:  1
    }
));

// -------------------------------------------------------
// QUERY 2: User's last 5 sessions
// -------------------------------------------------------
print("\n=== QUERY 2: Last 5 sessions for user 59472 ===");
printjson(db.user_engagement.findOne(
    { user_id: 59472 },
    {
        user_id:  1,
        sessions: { $slice: -5 }
    }
));

// ---------------------------------------------------------------------------------
// QUERY 3: Ad clicks per hour per campaign in 24 hours for a specific advertiser.
// ---------------------------------------------------------------------------------
print("\n=== QUERY 3: Clicks per hour for Advertiser_19 ===");
db.user_engagement.aggregate([
    { $match: {
        "sessions.impressions.advertiser_name": "Advertiser_19",
        "sessions.impressions.timestamp": {
            $gte: "2024-10-01T00:00:00",
            $lte: "2024-10-01T23:59:59"
        }
    }},
    { $unwind: "$sessions" },
    { $unwind: "$sessions.impressions" },
    { $match: {
        "sessions.impressions.advertiser_name": "Advertiser_19",
        "sessions.impressions.was_clicked": true,
        "sessions.impressions.timestamp": {
            $gte: "2024-10-01T00:00:00",
            $lte: "2024-10-01T23:59:59"
        }
    }},
    { $group: {
        _id: {
            campaign: "$sessions.impressions.campaign_name",
            hour:     { $substr: ["$sessions.impressions.timestamp", 0, 13] }
        },
        clicks: { $sum: 1 }
    }},
    { $sort: { "_id.hour": 1, "_id.campaign": 1 } }
]).forEach(doc => printjson(doc));

// -------------------------------
// QUERY 4: Ad fatigue detection
// -------------------------------
print("\n=== QUERY 4: Ad fatigue, or Users with 5+ impressions, but 0 clicks ===");
db.user_engagement.aggregate([
    { $unwind: "$sessions" },
    { $unwind: "$sessions.impressions" },
    { $group: {
        _id: {
            user_id:       "$user_id",
            campaign_name: "$sessions.impressions.campaign_name"
        },
        impression_count: { $sum: 1 },
        click_count: { $sum: { $cond: ["$sessions.impressions.was_clicked", 1, 0] } }
    }},
    { $match: {
        impression_count: { $gte: 5 },
        click_count: 0
    }},
    { $group: {
        _id:                "$_id.user_id",
        fatigued_campaigns: { $push: "$_id.campaign_name" },
        total_fatigued:     { $sum: 1 }
    }},
    { $sort: { total_fatigued: -1 } },
    { $limit: 10 }
]).forEach(doc => printjson(doc));

// -------------------------------------------------------
// QUERY 5: Top 3 most engaged ad categories per user
// -------------------------------------------------------
print("\n=== QUERY 5: Top 3 engaged campaigns for user 59472 ===");
db.user_engagement.aggregate([
    { $match: { user_id: 59472 } },
    { $unwind: "$sessions" },
    { $unwind: "$sessions.impressions" },
    { $match: { "sessions.impressions.was_clicked": true } },
    { $group: {
        _id:    "$sessions.impressions.campaign_name",
        clicks: { $sum: 1 }
    }},
    { $sort: { clicks: -1 } },
    { $limit: 3 },
    { $project: {
        campaign: "$_id",
        clicks:   1,
        _id:      0
    }}
]).forEach(doc => printjson(doc));