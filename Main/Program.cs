using System.Reactive.Linq;

namespace MyApp
{
    internal class Program
    {
        static IEnumerable<string[]> from_csv(string filename)
        {
            return File.ReadLines(filename)
                .Skip(1)
                .Select(s => s.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries));
        }

        static void Main(string[] args)
        {
            //var train_sessions = from_csv("/home/anandj/data/code/RecSys22/data/train_sessions.csv")
            var train_sessions = from_csv("/tmp/test.csv")
                .Select(s =>
                {
                    return new
                    {
                        session_id = s[0], item_id = s[1],
                        timestamp = DateTimeOffset.Parse(s[2]).ToUnixTimeMilliseconds()
                    };
                })
                .ToArray();
            /*
            var train_purchase = from_csv("/home/anandj/data/code/RecSys22/data/train_purchase.csv")
                .Select(s =>
                {
                    return new
                    {
                        session_id = s[0], item_id = s[1],
                        timestamp = DateTimeOffset.Parse(s[2]).ToUnixTimeMilliseconds()
                    };
                });
*/
            var session_features = train_sessions
                .OrderBy(e => e.timestamp)
                .GroupBy(e => e.session_id)
                .Select(e => new
                {
                    session_id = e.Key,
                    count = e.Select(s => s.item_id).Distinct().Count(),
                    start = e.Select(s => s.timestamp).Min(),
                    end = e.Select(s => s.timestamp).Max(),
                    last_item = e.Select(s => s.item_id).Last()
                });
            foreach (var r in session_features)
            {
                Console.WriteLine("{0}, {1}, {2}, {3}", r.session_id, r.count, r.end - r.start, r.last_item);
            }
        }
    }
}