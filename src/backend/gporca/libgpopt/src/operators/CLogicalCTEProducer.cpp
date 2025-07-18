//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalCTEProducer.cpp
//
//	@doc:
//		Implementation of CTE producer operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CLogicalCTEProducer.h"

#include "gpos/base.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::CLogicalCTEProducer
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalCTEProducer::CLogicalCTEProducer(CMemoryPool *mp)
	: CLogical(mp), m_id(0), m_pdrgpcr(nullptr), m_pcrsOutput(nullptr), 
	  m_cbp(false), m_umask(nullptr)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::CLogicalCTEProducer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalCTEProducer::CLogicalCTEProducer(CMemoryPool *mp, ULONG id,
										 CColRefArray *colref_array,
										 BOOL canbepruned)
	: CLogical(mp), m_id(id), m_pdrgpcr(colref_array), 
	  m_pcrsOutput(nullptr), m_cbp(canbepruned), m_umask(nullptr)
{
	GPOS_ASSERT(nullptr != colref_array);

	m_pcrsOutput = GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcr);
	GPOS_ASSERT(m_pdrgpcr->Size() == m_pcrsOutput->Size());

	m_pcrsLocalUsed->Include(m_pdrgpcr);
}

void 
CLogicalCTEProducer::RecalOutputColumns(BOOL *umask, ULONG sz) {
	CRefCount::SafeRelease(m_pcrsOutput);

	GPOS_ASSERT(m_pdrgpcr->Size() == sz);

	m_pcrsOutput = GPOS_NEW(m_mp) CColRefSet(m_mp);
	for (ULONG ul = 0; ul < sz; ul++) {
		if (umask[ul]) {
			m_pcrsOutput->Include((*m_pdrgpcr)[ul]);
		}
	}

	m_umask = umask;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::~CLogicalCTEProducer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalCTEProducer::~CLogicalCTEProducer()
{
	CRefCount::SafeRelease(m_pdrgpcr);
	CRefCount::SafeRelease(m_pcrsOutput);
	if (m_umask) {
		GPOS_DELETE_ARRAY(m_umask);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::DeriveOutputColumns
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalCTEProducer::DeriveOutputColumns(CMemoryPool *,		  //mp,
										 CExpressionHandle &  //exprhdl
)
{
	m_pcrsOutput->AddRef();
	return m_pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::DeriveNotNullColumns
//
//	@doc:
//		Derive not nullable output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalCTEProducer::DeriveNotNullColumns(CMemoryPool *mp,
										  CExpressionHandle &exprhdl) const
{
	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);
	pcrs->Include(m_pcrsOutput);
	pcrs->Intersection(exprhdl.DeriveNotNullColumns(0));

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalCTEProducer::DeriveKeyCollection(CMemoryPool *,	 // mp
										 CExpressionHandle &exprhdl) const
{
	return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalCTEProducer::DeriveMaxCard(CMemoryPool *,  // mp
								   CExpressionHandle &exprhdl) const
{
	// pass on max card of first child
	return exprhdl.DeriveMaxCard(0);
}

CTableDescriptorHashSet *
CLogicalCTEProducer::DeriveTableDescriptor(CMemoryPool *,
										   CExpressionHandle &exprhdl) const
{
	// pass on table descriptor of first child
	CTableDescriptorHashSet *child_table_descriptor_set =
		exprhdl.DeriveTableDescriptor(0);
	child_table_descriptor_set->AddRef();
	return child_table_descriptor_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalCTEProducer::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalCTEProducer *popCTEProducer = CLogicalCTEProducer::PopConvert(pop);

	return m_id == popCTEProducer->UlCTEId() &&
		   m_pdrgpcr->Equals(popCTEProducer->Pdrgpcr()) && 
		   m_cbp == popCTEProducer->CanBePruned();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalCTEProducer::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(COperator::HashValue(), m_id);
	ulHash = gpos::CombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));
	ulHash = gpos::CombineHashes(ulHash, (ULONG)m_cbp);

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalCTEProducer::PopCopyWithRemappedColumns(
	CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist)
{
	CColRefArray *colref_array =
		CUtils::PdrgpcrRemap(mp, m_pdrgpcr, colref_mapping, must_exist);

	return GPOS_NEW(mp) CLogicalCTEProducer(mp, m_id, colref_array);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalCTEProducer::PxfsCandidates(CMemoryPool *mp) const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementCTEProducer);
	return xform_set;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalCTEProducer::OsPrint(IOstream &os) const
{
	os << SzId() << " (";
	os << m_id;
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os << "]";

	return os;
}

// EOF
