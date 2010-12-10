#ifndef H_PUBLIC
#define H_PUBLIC

/*
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

			Public (Public const& p)
				: m_p(p.m_p)
			{
				m_p->Ref();
			}

			Public& operator= (Public const& p)
			{
				m_p = p.m_p;
				m_p->Ref();

				return *this;
			}

			~Public ()
			{
				m_p->Unref();
			}

			T* operator-> () const
			{
				return m_p;
			}

		protected:
			Public (T* p)
				: m_p(p)
			{
				m_p->Ref();
			}

			T* m_p;
	};
}
*/

#endif
