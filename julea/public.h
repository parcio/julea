#ifndef H_PUBLIC
#define H_PUBLIC

namespace JULEA
{
	template<typename T>
	class Public
	{
		public:
			Public ()
				: m_p(0)
			{
			}

			Public (Public const& collection)
				: m_p(collection.m_p)
			{
				m_p->Ref();
			}

			Public& operator= (Public const& collection)
			{
				m_p = collection.m_p;
				m_p->Ref();

				return *this;
			}

			~Public ()
			{
				cout << "unref " << m_p << " " <<  this << endl;
				m_p->Unref();
			}

			T* operator-> ()
			{
				return m_p;
			}

		protected:
			T* m_p;
	};
}

#endif
