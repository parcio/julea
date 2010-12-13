#ifndef H_ITEM
#define H_ITEM

struct JItem;

typedef struct JItem JItem;

/*
#include "collection.h"
#include "public.h"
#include "ref_counted.h"
#include "semantics.h"

namespace JULEA
{
	class _Item : public RefCounted<_Item>
	{
		friend class RefCounted<_Item>;

		friend class Item;

		friend class _Collection;

		public:
			string const& Name () const;

			_Semantics const* GetSemantics ();
			void SetSemantics (Semantics const&);
		private:
			_Item (string const&);
			_Item (_Collection*, mongo::BSONObj const&);
			~_Item ();

			void IsInitialized (bool);

			mongo::BSONObj Serialize ();
			void Deserialize (mongo::BSONObj const&);

			void Associate (_Collection*);

			bool m_initialized;

			mongo::OID m_id;
			string m_name;

			_Collection* m_collection;
			_Semantics* m_semantics;
	};

	class Item : public Public<_Item>
	{
		friend class Collection;
		friend class _Collection;

		public:
			Item (string const& name)
			{
				m_p = new _Item(name);
			}

		private:
			Item (_Collection* collection, mongo::BSONObj const& obj)
			{
				m_p = new _Item(collection, obj);
			}
	};
}
*/

#endif
