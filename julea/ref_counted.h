#ifndef H_REF_COUNTED
#define H_REF_COUNTED

#include <boost/utility.hpp>

namespace JULEA
{
	template<typename T>
	class RefCounted;
}

#include "ref_counted.h"

namespace JULEA
{
	template<typename T>
	class RefCounted : public boost::noncopyable
	{
		protected:
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
					delete this;

					return true;
				}

				return false;
			}

			unsigned int m_refCount;
	};
}

#endif
