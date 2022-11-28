using System;
using System.Linq;

namespace YesSql.Services
{
    public class DefaultTableNameConvention : ITableNameConvention
    {
        private readonly NameConventionOptions _options;

        public DefaultTableNameConvention(NameConventionOptions options)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        [Obsolete("Use NameConventionOptions.DocumentTableName instead")]
        public const string DocumentTable = "Document";

        public string GetIndexTable(Type type, string collection = null)
        {
            if (String.IsNullOrEmpty(collection))
            {
                return type.Name;
            }

            return collection + _options.TableSeperator + type.Name;
        }

        public string GetDocumentTable(string collection = null)
        {
            if (String.IsNullOrEmpty(collection))
            {
                return _options.DocumentTableName;
            }

            return GetTableName(collection, _options.DocumentTableName);
        }

        public string GetTableName(params string[] names)
        {
            var cleanedNames = names.Select(x => x?.Trim()).Where(x => !String.IsNullOrWhiteSpace(x));

            return String.Join(_options.TableSeperator ?? String.Empty, cleanedNames);
        }
    }
}
