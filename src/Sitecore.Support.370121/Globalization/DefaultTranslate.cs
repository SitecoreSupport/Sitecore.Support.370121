using Sitecore;
using Sitecore.Abstractions;
using Sitecore.Collections;
using Sitecore.Common;
using Sitecore.Data;
using Sitecore.Data.Items;
using Sitecore.Diagnostics;
using Sitecore.Diagnostics.PerformanceCounters;
using Sitecore.Globalization;
using Sitecore.IO;
using Sitecore.Pipelines.GetTranslation;
using Sitecore.SecurityModel;
using Sitecore.StringExtensions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Xml.Linq;

namespace Sitecore.Support.Globalization
{
    public class DefaultTranslate : Sitecore.Globalization.DefaultTranslate
    {
        /// <summary>
        /// The lock object.
        /// </summary>
        private readonly object lockObject = new object();

        /// <summary>
        /// The core pipeline factory.
        /// </summary>
        private readonly BaseCorePipelineManager corePipelineManager;

        /// <summary>
        /// The logger.
        /// </summary>
        private readonly BaseLog log;

        /// <summary>
        /// The domains.
        /// </summary>
        private Hashtable domainsTable;

        /// <summary>
        /// Indicates whether cache should be reloaded.
        /// </summary>
        private bool hasPendingReloads;

        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultTranslate"/> class.
        /// </summary>
        /// <param name="corePipelineManager">The core pipeline factory.</param>
        /// <param name="log">The logger.</param>
        public DefaultTranslate(BaseCorePipelineManager corePipelineManager, BaseLog log) : base(corePipelineManager, log)
        {
        }

        /// <summary>
        /// Gets a value indicating whether translation cache needs to be reloaded.
        /// </summary>
        /// <value>
        ///   <c>true</c> if translation cache needs to be reloaded; otherwise, <c>false</c>.
        /// </value>
        public override bool HasPendingReloads
        {
            get
            {
                return this.hasPendingReloads;
            }
        }

        /// <summary>
        /// Gets the domains.
        /// </summary>
        /// <value>The domains.</value>
        [NotNull]
        private Hashtable Domains
        {
            get
            {
                if (this.domainsTable != null)
                {
                    return this.domainsTable;
                }

                lock (this.lockObject)
                {
                    if (this.domainsTable == null)
                    {
                        this.domainsTable = this.Load() ?? new Hashtable();
                    }
                }

                return this.domainsTable;
            }
        }

        /// <summary>
        /// Removes the key from cache.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="language">The language.</param>
        /// <param name="domain">The domain.</param>
        public override void RemoveKeyFromCache([NotNull] string key, [NotNull] Language language, [NotNull] DictionaryDomain domain)
        {
            Assert.ArgumentNotNull(key, "key");
            Assert.ArgumentNotNull(language, "language");
            Assert.ArgumentNotNull(domain, "domain");

            var languages = this.GetLanguageTable(domain);

            if (language == Language.Invariant)
            {
                lock (this.lockObject)
                {
                    foreach (string languageName in languages.Keys)
                    {
                        var phrases = languages[languageName] as Hashtable;
                        if (phrases != null)
                        {
                            phrases.Remove(key);
                        }
                    }
                }
            }
            else
            {
                var phrases = languages[language.ToString()] as Hashtable;
                if (phrases != null)
                {
                    lock (this.lockObject)
                    {
                        phrases.Remove(key);
                    }
                }
            }
        }

        /// <summary>
        /// Removes the key from cache.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="language">The language.</param>
        public override void RemoveKeyFromCache([NotNull] string key, [NotNull] Language language)
        {
            Assert.ArgumentNotNull(key, "key");
            Assert.ArgumentNotNull(language, "language");

            foreach (var domain in this.GetCachedDomains())
            {
                this.RemoveKeyFromCache(key, language, domain);
            }
        }

        /// <summary>
        /// Removes the key from cache.
        /// </summary>
        /// <param name="key">The key.</param>
        public override void RemoveKeyFromCache([NotNull] string key)
        {
            Assert.ArgumentNotNull(key, "key");

            this.RemoveKeyFromCache(key, Language.Invariant);
        }

        /// <summary>
        /// Caches the phrase.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="phrase">The phrase.</param>
        /// <param name="language">The language.</param>
        /// <param name="domain">The domain.</param>
        public override void CachePhrase([NotNull] string key, [NotNull] string phrase, [NotNull] Language language, [NotNull] DictionaryDomain domain)
        {
            Assert.ArgumentNotNull(key, "key");
            Assert.ArgumentNotNull(phrase, "phrase");
            Assert.ArgumentNotNull(language, "language");
            Assert.ArgumentNotNull(domain, "domain");

            if (language == Language.Invariant)
            {
                this.log.Warn(string.Format("Cannot cache phrase for invariant language:\nKey:{0}\nPhrase:{1}", key, phrase), new object());
                return;
            }

            var phrases = this.GetPhrases(domain, language);
            lock (this.lockObject)
            {
                phrases[key] = phrase;
            }
        }

        /// <summary>
        /// Gets the cached domains.
        /// </summary>
        /// <returns>The array of cached domains.</returns>
        [NotNull]
        public override DictionaryDomain[] GetCachedDomains()
        {
            var domains = new List<DictionaryDomain>();
            lock (this.lockObject)
            {
                foreach (string key in this.Domains.Keys)
                {
                    DictionaryDomain domain;

                    if (!DictionaryDomain.TryParse(key, out domain))
                    {
                        continue;
                    }

                    domains.Add(domain);
                }
            }

            return domains.ToArray();
        }

        /// <summary>
        /// Gets the cached languages.
        /// </summary>
        /// <param name="domain">The domain.</param>
        /// <returns>The array of cached languages for specified dictionary domain.</returns>
        [NotNull]
        public override Language[] GetCachedLanguages([NotNull] DictionaryDomain domain)
        {
            Assert.ArgumentNotNull(domain, "domain");
            var languages = new List<Language>();
            string key = domain.FullyQualifiedName;

            var languagesTable = this.Domains[key] as Hashtable;
            if (languagesTable == null)
            {
                return languages.ToArray();
            }

            lock (this.Domains.SyncRoot)
            {
                foreach (string languageName in languagesTable.Keys)
                {
                    Language language;
                    if (Language.TryParse(languageName, out language))
                    {
                        languages.Add(language);
                    }
                }
            }

            return languages.ToArray();
        }

        /// <summary>
        /// Creates the dictionary.
        /// </summary>
        /// <param name="type">
        /// The type containing the strings for which the dictionary should be created.
        /// </param>
        /// <returns>
        /// The dictionary.
        /// </returns>
        [NotNull]
        public override XDocument CreateDictionary([NotNull] Type type)
        {
            Assert.IsNotNull(type, typeof(Type));

            var xmlDoc = new XDocument(new XDeclaration("1.0", "utf-8", null));
            var sitecoreNode = new XElement("sitecore");
            xmlDoc.Add(sitecoreNode);

            foreach (FieldInfo info in type.GetFields())
            {
                var value = info.GetRawConstantValue() as string;
                if (string.IsNullOrEmpty(value))
                {
                    continue;
                }

                var phrase = new XElement("phrase")
                {
                    Value = this.Text(value)
                };
                phrase.Add(new XAttribute("key", value));
                sitecoreNode.Add(phrase);
            }

            return xmlDoc;
        }

        /// <summary>
        /// Gets a localized string.
        /// </summary>
        /// <param name="key">
        /// The key of the string.
        /// </param>
        /// <returns>
        /// The localized string.
        /// </returns>
        /// <remarks>
        /// If the key is not found, the key itself is returned.
        /// </remarks>
        /// <example>
        /// <code lang="CS">
        /// string myText = Localization.ls("My Text");
        /// </code>
        /// </example>
        [NotNull]
        public override string Text([NotNull] string key)
        {
            Assert.ArgumentNotNull(key, "key");

            return this.TextByLanguage(key, Context.Language);
        }

        /// <summary>
        /// Gets a localized string.
        /// </summary>
        /// <param name="domain">The domain.</param>
        /// <param name="options">The options.</param>
        /// <param name="key">The key of the string.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>
        /// The localized string.
        /// </returns>
        [NotNull]
        public override string TextByDomain([CanBeNull] string domain, [NotNull] TranslateOptions options, [NotNull] string key, [NotNull] params object[] parameters)
        {
            Assert.ArgumentNotNull(options, "options");
            Assert.ArgumentNotNull(key, "key");

            return this.TextByLanguage(domain, options, key, Context.Language, key, parameters) ?? key;
        }

        /// <summary>
        /// Gets a localized string.
        /// </summary>
        /// <param name="domain">The domain.</param>
        /// <param name="key">The key of the string.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>
        /// The localized string.
        /// </returns>
        [NotNull]
        public override string TextByDomain([CanBeNull] string domain, [NotNull] string key, [NotNull] params object[] parameters)
        {
            Assert.ArgumentNotNull(key, "key");

            return this.TextByDomain(domain, TranslateOptions.Default, key, parameters);
        }

        /// <summary>
        /// Gets the localized string for the key.
        /// </summary>
        /// <param name="key">
        /// The text key.
        /// </param>
        /// <param name="parameters">
        /// The parameters.
        /// </param>
        /// <returns>
        /// The translated text for given key.
        /// </returns>
        /// <remarks>
        /// The localized string is formatted using String.Format.
        /// </remarks>
        [NotNull]
        public override string Text([NotNull] string key, [NotNull] params object[] parameters)
        {
            Assert.ArgumentNotNull(key, "key");
            Assert.ArgumentNotNull(parameters, "parameters");

            string result = this.Text(key);

            if (parameters.Length > 0)
            {
                return string.Format(result, parameters);
            }

            return result;
        }

        /// <summary>
        /// Gets a localized string.
        /// </summary>
        /// <param name="key">
        /// The key of the string.
        /// </param>
        /// <param name="language">
        /// The language identifier.
        /// </param>
        /// <returns>
        /// The localized string.
        /// </returns>
        /// <remarks>
        /// If the key is not found, the key itself is returned.
        /// </remarks>
        /// <example>
        /// <code lang="CS">
        /// string myText = Translate.Text("My Text", "da");
        /// </code>
        /// </example>
        [NotNull]
        public override string TextByLanguage([NotNull] string key, [NotNull] Language language)
        {
            Assert.ArgumentNotNull(key, "key");
            Assert.ArgumentNotNull(language, "language");

            return this.TextByLanguage(key, language, key) ?? string.Empty;
        }

        /// <summary>
        /// Texts the by language no default.
        /// </summary>
        /// <param name="key">
        /// The text key.
        /// </param>
        /// <param name="language">
        /// The language.
        /// </param>
        /// <param name="defaultValue">
        /// The default value.
        /// </param>
        /// <returns>
        /// The by language.
        /// </returns>
        [CanBeNull]
        public override string TextByLanguage([NotNull] string key, [NotNull] Language language, [CanBeNull] string defaultValue)
        {
            Assert.ArgumentNotNull(key, "key");
            Assert.ArgumentNotNull(language, "language");

            return this.TextByLanguage(key, language, defaultValue, null) ?? defaultValue;
        }

        /// <summary>
        /// Texts the by language no default.
        /// </summary>
        /// <param name="key">The text key.</param>
        /// <param name="language">The language.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <param name="parameters">String replacement parameters.</param>
        /// <returns>The by text language.</returns>
        [CanBeNull]
        public override string TextByLanguage([CanBeNull] string key, [NotNull] Language language, [CanBeNull] string defaultValue, [CanBeNull] params object[] parameters)
        {
            Assert.ArgumentNotNull(language, "language");

            return this.TextByLanguage(null, TranslateOptions.Default, key, language, defaultValue, parameters);
        }

        /// <summary>
        /// Texts the by language.
        /// </summary>
        /// <param name="domainName">Name of the domain.</param>
        /// <param name="options">The options.</param>
        /// <param name="key">The phrase key.</param>
        /// <param name="language">The language.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>The translated phrase for specified combination of key, language, domain.</returns>
        [CanBeNull]
        public override string TextByLanguage([CanBeNull] string domainName, [NotNull] TranslateOptions options, [CanBeNull] string key, [NotNull] Language language, [CanBeNull] string defaultValue, [CanBeNull] params object[] parameters)
        {
            Assert.ArgumentNotNull(options, "options");
            Assert.ArgumentNotNull(language, "language");

            string translationPipelineName = "getTranslation";
            string result = null;

            if (this.corePipelineManager.GetPipeline(translationPipelineName, string.Empty) != null)
            {
                var args = new GetTranslationArgs
                {
                    Language = language,
                    Key = key,
                    Options = options,
                    DomainName = domainName,
                    Parameters = parameters
                };
                this.corePipelineManager.Run(translationPipelineName, args, false);
                result = args.Result;
            }
            else
            {
                this.log.Error(string.Format("Cannot find translation pipeline with name '{0}'.", translationPipelineName), typeof(Translate));
            }

            return result ?? (parameters == null || defaultValue == null ? defaultValue : string.Format(defaultValue, parameters));
        }

        /// <summary>
        /// Resets the cache.
        /// </summary>
        public override void ResetCache()
        {
            if (DictionaryBatchOperationContext.CurrentValue)
            {
                lock (this.lockObject)
                {
                    this.hasPendingReloads = true;
                    return;
                }
            }

            this.ResetCache(true);
        }

        /// <summary>
        /// Resets the cache.
        /// </summary>
        /// <param name="removeFileCache">If set to <c>true</c> dictionary file will be removed.</param>
        public override void ResetCache(bool removeFileCache)
        {
            if (DictionaryBatchOperationContext.CurrentValue)
            {
                lock (this.lockObject)
                {
                    this.hasPendingReloads = true;
                    return;
                }
            }

            try
            {
                if (removeFileCache)
                {
                    string fileName = this.GetFilename();
                    if (File.Exists(fileName))
                    {
                        File.Delete(fileName);
                    }
                }

                this.Domains.Clear();
            }
            catch (Exception ex)
            {
                this.log.Error("Cannot reset translation cache.", ex, typeof(Translate));
            }

            lock (this.lockObject)
            {
                this.hasPendingReloads = false;
            }
        }

        /// <summary>
        /// Reloads the domain cache.
        /// </summary>
        /// <param name="domain">The domain.</param>
        public override void ReloadDomainCache([NotNull] DictionaryDomain domain)
        {
            Assert.ArgumentNotNull(domain, "domain");

            if (DictionaryBatchOperationContext.CurrentValue)
            {
                lock (this.lockObject)
                {
                    this.hasPendingReloads = true;
                    return;
                }
            }

            lock (this.lockObject)
            {
                if (!this.Domains.ContainsKey(domain.FullyQualifiedName))
                {
                    return;
                }
            }

            var languages = new List<string>();
            lock (this.lockObject)
            {
                var l = this.Domains[domain.FullyQualifiedName] as Hashtable;
                if (l != null)
                {
                    foreach (string langName in l.Keys)
                    {
                        languages.Add(langName);
                    }
                }

                this.Domains.Remove(domain.FullyQualifiedName);
            }

            foreach (string langName in languages)
            {
                this.Load(domain, langName);
            }
        }

        /// <summary>
        /// Reloads the translations from database.
        /// </summary>
        public override void ReloadFromDatabase()
        {
            if (DictionaryBatchOperationContext.CurrentValue)
            {
                lock (this.lockObject)
                {
                    this.hasPendingReloads = true;
                    return;
                }
            }

            var loadedDomains = new Dictionary<string, List<string>>();
            lock (this.lockObject)
            {
                foreach (string domainName in this.Domains.Keys)
                {
                    loadedDomains.Add(domainName, new List<string>());
                    var languages = this.Domains[domainName] as Hashtable;
                    if (languages != null)
                    {
                        foreach (string language in languages.Keys)
                        {
                            loadedDomains[domainName].Add(language);
                        }
                    }
                }

                this.ResetCache(true);
            }

            foreach (var domainName in loadedDomains.Keys)
            {
                foreach (string language in loadedDomains[domainName])
                {
                    DictionaryDomain domain;
                    if (DictionaryDomain.TryParse(domainName, out domain))
                    {
                        this.Load(domain, language);
                    }
                }
            }
        }

        /// <summary>
        /// Texts the by language.
        /// </summary>
        /// <param name="key">The phrase key.</param>
        /// <param name="domain">The domain.</param>
        /// <param name="language">The language.</param>
        /// <param name="phrase">The phrase.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns>
        ///   <c>true</c> if dictionary entry for specified key has been found in specified domain and language.
        /// </returns>
        /// <remarks>
        /// Do not use this method unless you are sure that you need to get translations from translation cache without using fallback logic.
        /// </remarks>
        public override bool TryTranslateTextByLanguage([CanBeNull] string key, [NotNull] DictionaryDomain domain, [NotNull] Language language, [CanBeNull] out string phrase, [CanBeNull] params object[] parameters)
        {
            Assert.ArgumentNotNull(domain, "domain");
            Assert.ArgumentNotNull(language, "language");

            phrase = null;

            if (string.IsNullOrEmpty(key))
            {
                return false;
            }

            var lang = this.GetPhrases(domain, language);

            var text = string.Empty;

            lock (this.lockObject)
            {
                text = lang[key] as string;
            }

            if (!string.IsNullOrEmpty(text))
            {
                DataCount.GlobalizationTextsTranslated.Increment();
                phrase = parameters == null || parameters.Length == 0 ? text : string.Format(text, parameters);
                return true;
            }

            DataCount.GlobalizationTranslateFailed.Increment();
            return false;
        }

        /// <summary>
        /// Saves this instance.
        /// </summary>
        public override void Save()
        {
            string filename = this.GetFilename();

            lock (FileUtil.GetFileLock(filename))
            {
                try
                {
                    FileUtil.Delete(filename);
                }
                catch (Exception ex)
                {
                    this.log.Error(string.Format("Error delete {0}.", filename), ex, typeof(Translate));
                }

                try
                {
                    TempFolder.EnsureFolder();

                    var formatter = new BinaryFormatter();

                    using (var stream = new FileStream(filename, FileMode.CreateNew, FileAccess.Write, FileShare.Write, 8192))
                    {
                        formatter.Serialize(stream, this.Domains);
                    }
                }
                catch (Exception ex)
                {
                    this.log.Error(string.Format("Error saving {0}.", filename), ex, typeof(Translate));
                    FileUtil.Delete(filename);
                }
            }
        }

        /// <summary>
        /// Gets the file name.
        /// </summary>
        /// <returns>
        /// The get file name.
        /// </returns>
        [NotNull]
        internal string GetFilename()
        {
            return FileUtil.MapPath(FileUtil.MakePath(TempFolder.Folder, "dictionary.dat"));
        }

        /// <summary>
        /// Loads this instance.
        /// </summary>
        /// <returns>
        /// The table of translation phrases. 
        /// </returns>
        [CanBeNull]
        private Hashtable Load()
        {
            string filename = this.GetFilename();

            if (!FileUtil.Exists(filename))
            {
                return null;
            }

            try
            {
                FileAttributes fileAttributes = File.GetAttributes(filename);
                if ((fileAttributes & FileAttributes.ReadOnly) == FileAttributes.ReadOnly)
                {
                    this.log.Info(string.Format("{0} is read-only. Rebuilding cache from database.", filename), typeof(Translate));
                    return null;
                }
            }
            catch (Exception ex)
            {
                this.log.Error(string.Format("Failed to get file attributes on {0}. Rebuilding cache from database.", filename), ex, typeof(Translate));
                return null;
            }

            lock (FileUtil.GetFileLock(filename))
            {
                try
                {
                    this.log.Info("Loading Dictionary from cache", typeof(Translate));

                    var formatter = new BinaryFormatter();

                    using (var stream = new FileStream(filename, FileMode.Open, FileAccess.Read, FileShare.Read, 8192))
                    {
                        return formatter.Deserialize(stream) as Hashtable;
                    }
                }
                catch (Exception ex)
                {
                    this.log.Error("Error loading data.", ex, typeof(Translate));
                }
            }

            return null;
        }

        /// <summary>
        /// Gets the language table.
        /// </summary>
        /// <param name="domain">The domain.</param>
        /// <returns>The cache for the domain.</returns>
        [NotNull]
        private Hashtable GetLanguageTable([NotNull] DictionaryDomain domain)
        {
            Assert.ArgumentNotNull(domain, "domain");
            Hashtable hashtable;

            lock (this.lockObject)
            {
                hashtable = this.Domains[domain.FullyQualifiedName] as Hashtable;
            }

            return hashtable ?? new Hashtable();
        }

        /// <summary>
        /// Gets the phrases.
        /// </summary>
        /// <param name="domain">The domain.</param>
        /// <param name="language">The language.</param>
        /// <returns>The phrases.</returns>
        [NotNull]
        private Hashtable GetPhrases([NotNull] DictionaryDomain domain, [NotNull] Language language)
        {
            Assert.ArgumentNotNull(domain, "domain");
            Assert.ArgumentNotNull(language, "language");

            Hashtable languageTable = this.GetLanguageTable(domain)[language.ToString()] as Hashtable;

            return (languageTable != null && languageTable.Keys.Count > 0) ? languageTable : this.Load(domain, language.ToString());
        }

        /// <summary>
        /// Loads the specified language.
        /// </summary>
        /// <param name="domain">The domain.</param>
        /// <param name="language">The language.</param>
        /// <returns>The table of translation phrases.</returns>
        [NotNull]
        private Hashtable Load([NotNull] DictionaryDomain domain, [NotNull] string language)
        {
            Assert.IsNotNull(domain, "domain");
            Assert.ArgumentNotNull(language, "language");

            Hashtable languages;
            var result = new Hashtable();
            lock (this.lockObject)
            {
                if (!this.Domains.ContainsKey(domain.FullyQualifiedName) || (this.Domains[domain.FullyQualifiedName] as Hashtable) == null)
                {
                    languages = new Hashtable();
                    this.Domains[domain.FullyQualifiedName] = languages;
                }
                else
                {
                    languages = this.Domains[domain.FullyQualifiedName] as Hashtable;
                    Assert.IsNotNull(languages, "languages");
                }

                result = languages[language] as Hashtable;
                if (result != null && result.Keys.Count > 0)
                {
                    // the dictionary has been loaded in a different thread while current one has been locked.
                    return result;
                }

                result = new Hashtable();
                languages[language] = result;

                try
                {
                    using (new SecurityDisabler())
                    {
                        Item item = domain.GetDefinitionItem(Language.Parse(language));
                        if (item != null)
                        {
                            this.log.Info(string.Format("Loading Dictionary from database. Domain: '{0}'. Language: '{1}'.", domain.Name, language), typeof(Translate));
                            this.Load(result, item, true);
                        }
                    }

                    if (result.Keys.Count == 0)
                    {
                        result.Add(ID.NewID, ID.NewID);
                    }

                    this.Save();
                }
                catch (InvalidOperationException exception)
                {
                    this.log.Warn("Failed to load Dictionary (language: {0}). Error message: {1}".FormatWith(language, exception.Message), new object());
                }
            }

            return result;
        }

        /// <summary>
        /// Loads the specified table.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <param name="item">The dictionary item.</param>
        /// <param name="rootItem">Value of root item indicates whether this is initial or recursive call of the method.</param>
        private void Load([NotNull] Hashtable table, [NotNull] Item item, bool rootItem)
        {
            Assert.ArgumentNotNull(table, "table");
            Assert.ArgumentNotNull(item, "item");

            TemplateItem template = item.Template;
            if (template == null)
            {
                return;
            }

            if (template.Key == "dictionary entry" && (item.IsFallback || item.Versions.GetVersions(false).Length > 0))
            {
                string key = item["Key"].Replace("\r\n", "\n").Replace("\\n", "\n");
                string value = item["Phrase"];

                value = value.Replace("\r\n", "\n").Replace("\\n", "\n").Replace("&lt;", "<").Replace("&gt;", ">");
                table[key] = value;
            }

            if (rootItem || template.ID != TemplateIDs.DictionaryDomain)
            {
                foreach (Item child in item.GetChildren(ChildListOptions.None))
                {
                    this.Load(table, child, false);
                }
            }
        }
    }
}