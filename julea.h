#include <exception>
#include <list>
#include <memory>

#include <boost/utility.hpp>

#include <mongo/client/connpool.h>
#include <mongo/db/jsobj.h>

namespace JULEA
{
	class Collection;
	class Item;

	class Exception : public std::exception
	{
		public:
			Exception (string const&) throw();
			~Exception () throw();
			const char* what () const throw();
		private:
			string description;
	};

	class Store
	{
		public:
			static void Initialize (string const&);

			static string const& Host ();
		private:
			static string m_host;
	};

	class Credentials
	{
		public:
			Credentials ();
			~Credentials ();
		private:
			unsigned int user;
			unsigned int group;
	};

	class Item /*: public boost::noncopyable*/
	{
		friend class Collection;

		public:
			Item (Collection const&, string const&);
			~Item ();

			string const& Name () const;
		private:
			mongo::BSONObj Serialize ();
			void Deserialize (mongo::BSONObj const&);

			mongo::OID m_id;
			mongo::OID m_collectionID;
			string m_name;
	};

	class Collection
	{
		friend class Item;

		public:
			/*
			class Iterator
			{
				public:
					Iterator (Collection const& directory) : connection(FileSystem::Host()), directory(directory) { cursor =  connection->query("JULEA.DirectoryEntries", mongo::BSONObjBuilder().append("Collection", directory.m_id).obj()); };
					~Iterator () { connection.done(); };

					bool More () { return cursor->more(); };
					Item Next () { mongo::BSONObj f; string name; f = cursor->next(); name = f.getField("Name").String(); return Item(name); };
				private:
					mongo::ScopedDbConnection connection;
					Collection const& directory;
					std::auto_ptr<mongo::DBClientCursor> cursor;
			};
			*/

		public:
			Collection (string const&);
			~Collection ();

			Item Add (string const&);
		private:
			mongo::BSONObj Serialize ();
			void Deserialize (mongo::BSONObj const&);

			mongo::OID const& ID () const;

			mongo::OID m_id;
			string m_name;
			std::list<Item> newItems;
	};
}
