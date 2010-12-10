#ifndef H_REF_COUNTED
#define H_REF_COUNTED

/*
#include <boost/utility.hpp>

namespace JULEA
{
	template<typename T>
	class RefCounted : public boost::noncopyable
	{
		public:
			RefCounted ()
				: m_refCount(1)
			{
			}

			~RefCounted()
			{
			}

			T* Ref ()
			{
				m_refCount++;

				return static_cast<T*>(this);
			}

			bool Unref ()
			{
				m_refCount--;

				if (m_refCount == 0)
				{
					delete static_cast<T*>(this);

					return true;
				}

				return false;
			}

		protected:
			unsigned int m_refCount;
	};
}
*/

#endif
