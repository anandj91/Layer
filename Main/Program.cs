using System.Collections.Immutable;
using System.Diagnostics;
using System.Text;

namespace MyApp
{
    public static class MissingExtensions
    {
        public static int getIndex(this Dictionary<string, int> map, string key)
        {
            if (!map.ContainsKey(key))
            {
                int size = map.Count;
                map[key] = size;
            }

            return map[key];
        }
    }

    internal class Program
    {
        class Session
        {
            public int session_id;
            public int item_id;
            public long timestamp;

            public Session(int sessionId, int itemId, long timestamp)
            {
                session_id = sessionId;
                item_id = itemId;
                this.timestamp = timestamp;
            }
        };

        class Feature
        {
            public int item_id;
            public int category;
            public int value;

            public Feature(int itemId, int category, int value)
            {
                item_id = itemId;
                this.category = category;
                this.value = value;
            }
        }

        class ItemCache
        {
            public int item_id;
            public int session_count;
            public float session_avg_len;
            public float session_avg_dur;
            public int buy_count;
            public float bought_session_avg_len;
            public float bought_session_avg_dur;
            public float buy_percent;

            public ItemCache(int itemId, int sessionCount, float sessionAvgLen, float sessionAvgDur, int buyCount,
                float boughtSessionAvgLen, float boughtSessionAvgDur, float buyPercent)
            {
                item_id = itemId;
                session_count = sessionCount;
                session_avg_len = sessionAvgLen;
                session_avg_dur = sessionAvgDur;
                buy_count = buyCount;
                bought_session_avg_len = boughtSessionAvgLen;
                bought_session_avg_dur = boughtSessionAvgDur;
                buy_percent = buyPercent;
            }
        }

        public static long TRAIN_START_DATE = DateTimeOffset.Parse("2020-01-01 00:00:00").ToUnixTimeMilliseconds();
        public static long MONTH = 2678400000;

        static IEnumerable<string[]> from_csv(string filename)
        {
            return File.ReadLines(filename)
                .Skip(1)
                .Select(s => s.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries));
        }

        static IEnumerable<Session> session_from_csv(string filename)
        {
            return from_csv(filename)
                .Select(s => new Session(int.Parse(s[0]), int.Parse(s[1]),
                    DateTimeOffset.Parse(s[2]).ToUnixTimeMilliseconds()))
                .ToArray();
        }

        static IEnumerable<Feature> feature_from_csv(string filename)
        {
            return from_csv(filename)
                .Select(s => new Feature(int.Parse(s[0]), int.Parse(s[1]), int.Parse(s[2])))
                .ToArray();
        }

        public static T[] Shuffle<T>(T[] arr, int seed)
        {
            var rng = new Random(seed);
            return arr.OrderBy(_ => rng.Next()).ToArray();
        }

        static void Main()
        {
            var trainSessions = session_from_csv("/home/anandj/data/code/Layer/dressipi_recsys2022/train_sessions.csv")
                .AsParallel();
            var trainPurchase = session_from_csv("/home/anandj/data/code/Layer/dressipi_recsys2022/train_purchases.csv")
                .AsParallel();
            var itemFeatures =
                feature_from_csv("/home/anandj/data/code/RecSys22/data/dressipi_recsys2022/item_features.csv")
                    .AsParallel();

            var sessions = trainSessions
                .GroupBy(e => e.session_id)
                .Select(e => new
                {
                    session_id = e.Key,
                    item_ids = e.Select(s => s.item_id),
                    times = e.Select(s => s.timestamp),
                })
                .Join(trainPurchase, sess => sess.session_id, target => target.session_id,
                    (sess, target) => new
                    {
                        win = (sess.times.Min() - TRAIN_START_DATE) / MONTH,
                        sess.session_id,
                        item_ids = sess.item_ids.Append(target.item_id).ToArray(),
                        times = sess.times.Append(target.timestamp).ToArray(),
                        count = sess.item_ids.Distinct().Count(),
                        duration = sess.times.Max() - sess.times.Min(),
                        last_item_id = sess.item_ids.Last(),
                        target_item_id = target.item_id,
                        target_timestamp = target.timestamp
                    })
                .GroupBy(e => e.win)
                .Select(e => new { win = e.Key, sessions = e.ToArray() })
                .ToImmutableDictionary(e => e.win, e => e.sessions);
            Console.WriteLine("sessions Count: {0}", sessions.Count);

            var itemCache = sessions
                .AsParallel()
                .Select(e => new
                {
                    win = e.Key,
                    item_cache = e.Value
                        .SelectMany(s => s.item_ids.Zip(s.times, (item_id, timestamp) =>
                            new { s.session_id, item_id, timestamp, s.target_item_id, s.count, s.duration })
                        )
                        .GroupBy(s => s.item_id)
                        .Select(s =>
                        {
                            var unique_sessions = s.DistinctBy(p => p.session_id).ToArray();
                            var unique_targets = unique_sessions.Where(p => p.target_item_id == s.Key).ToArray();
                            var session_count = unique_sessions.Count();
                            var session_len = unique_sessions.Sum(p => p.count);
                            var session_dur = unique_sessions.Sum(p => p.duration);
                            var buy_count = unique_targets.Count();
                            var bought_session_len = unique_targets.Sum(p => p.count);
                            var bought_session_dur = unique_targets.Sum(p => p.count);
                            return new ItemCache(
                                s.Key,
                                session_count,
                                (session_count > 0) ? session_len / session_count : 0,
                                (session_count > 0) ? session_dur / session_count : 0,
                                buy_count,
                                (buy_count > 0) ? bought_session_len / buy_count : 0,
                                (buy_count > 0) ? bought_session_dur / buy_count : 0,
                                (session_count > 0) ? buy_count / session_count : 0);
                        })
                        .ToImmutableDictionary(s => s.item_id)
                })
                .ToImmutableDictionary(s => s.win, s => s.item_cache);
            Console.WriteLine("ItemCache Count: {0}", itemCache.Count);

            var candidates = sessions
                .AsParallel()
                .Select(e => new
                {
                    win = e.Key,
                    candidates = e.Value.Select(s => s.target_item_id).ToArray()
                })
                .ToImmutableDictionary(e => e.win, e => e.candidates);
            Console.WriteLine("candidates Count: {0}", candidates.Count);

            var itemCount = itemFeatures.Select(e => e.item_id).Distinct().Count();
            Console.WriteLine("itemCount: {0}", itemCount);

            var features = itemFeatures
                .GroupBy(e => e.item_id)
                .SelectMany(e =>
                {
                    var item_id = e.Key;
                    var cats = e.Select(s => s.category.ToString()).GroupBy(s => s)
                        .Select(s => new { col = s.Key, count = s.Count() });
                    var catVals = e.Select(s => s.category + "-" + s.value).GroupBy(s => s)
                        .Select(s => new { col = s.Key, count = s.Count() });
                    return cats.Concat(catVals).Select(s => new { item_id, s.col, s.count });
                });

            var colIndexMap = features.Select(e => e.col).Distinct()
                .Select((col, idx) => new { col, idx }).ToImmutableDictionary(e => e.col, e => e.idx);
            Console.WriteLine("colIndexMap count: {0}", colIndexMap.Count);

            var selFeatures = features
                .GroupBy(e => e.col)
                .Where(e => e.Sum(s => s.count) > itemCount * 0.05)
                .SelectMany(e => e.ToArray())
                .GroupBy(e => e.item_id)
                .ToImmutableDictionary(e => e.Key, e => e.Select(s => new { s.col, s.count }).ToArray());
            Console.WriteLine("selFeatures Count: {0}", selFeatures.Count);

            var xgbNCandidates = 20;
            var item_cache_fn = (int offset, ItemCache cache) =>
            {
                var builder = new StringBuilder();
                builder.Append(" ").Append(offset++).Append(":").Append(cache.session_count);
                builder.Append(" ").Append(offset++).Append(":").Append(cache.buy_count);
                builder.Append(" ").Append(offset++).Append(":").Append(cache.buy_percent);
                builder.Append(" ").Append(offset++).Append(":").Append(cache.session_avg_len);
                builder.Append(" ").Append(offset++).Append(":").Append(cache.session_avg_dur);
                builder.Append(" ").Append(offset++).Append(":").Append(cache.bought_session_avg_len);
                builder.Append(" ").Append(offset++).Append(":").Append(cache.bought_session_avg_dur);
                return (builder.ToString(), offset);
            };

            var item_feature_fn = (int offset, int item_id) =>
            {
                var builder = new StringBuilder();
                foreach (var f in selFeatures[item_id])
                {
                    builder.Append(" ").Append(offset + colIndexMap[f.col]).Append(":").Append(f.count);
                    offset++;
                }

                return (builder.ToString(), offset);
            };

            var timer = new Stopwatch();
            timer.Start();
            foreach (var group in sessions)
            {
                var sess = group.Value;
                var icache = itemCache[group.Key];
                var cand = candidates[group.Key];
                var lines = new List<string>[sess.Length];
                Parallel.For(0, sess.Length, i =>
                {
                    var s = sess[i];
                    lines[i] = new List<string>();
                    var builder = new StringBuilder();
                    int offset = 0;
                    string last_item_cache_str;
                    string last_item_feature_str;
                    string target_item_cache_str;
                    string target_item_feature_str;
                    string candidate_item_cache_str;
                    string candidate_item_feature_str;

                    builder.Append(" ").Append(offset++).Append(":").Append(s.count);
                    builder.Append(" ").Append(offset++).Append(":").Append((s.duration) / 3600000.0);

                    (last_item_cache_str, offset) = item_cache_fn(offset, icache[s.last_item_id]);
                    (last_item_feature_str, offset) = item_feature_fn(offset, s.last_item_id);
                    builder.Append(last_item_cache_str);
                    builder.Append(last_item_feature_str);

                    (target_item_cache_str, offset) = item_cache_fn(offset, icache[s.target_item_id]);
                    (target_item_feature_str, offset) = item_feature_fn(offset, s.target_item_id);
                    builder.Append(target_item_cache_str);
                    builder.Append(target_item_feature_str);

                    var sampled = cand.Except(s.item_ids).Take(xgbNCandidates - 1).OrderBy(e => e);
                    foreach (var c in sampled)
                    {
                        (candidate_item_cache_str, offset) = item_cache_fn(offset, icache[c]);
                        (candidate_item_feature_str, offset) = item_feature_fn(offset, c);

                        var line = string.Format("{0} qid:{1}", (c == s.target_item_id) ? 1 : 0, s.session_id) +
                                   builder + candidate_item_cache_str + candidate_item_feature_str;
                        lines[i].Add(line);
                    }
                });

                using (var writer = new StreamWriter("/home/anandj/data/code/Layer/session_features.svm"))
                {
                    foreach (var line in lines)
                    {
                        foreach (var l in line)
                        {
                            writer.WriteLine(l);
                        }
                    }
                }
            }

            timer.Stop();
            Console.WriteLine("Time: {0}", timer.ElapsedMilliseconds / 1000.0);
        }
    }
}