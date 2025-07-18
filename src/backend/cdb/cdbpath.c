/*-------------------------------------------------------------------------
 *
 * cdbpath.c
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbpath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "nodes/makefuncs.h"	/* makeFuncExpr() */
#include "nodes/nodeFuncs.h"	/* exprType() */
#include "nodes/pathnodes.h"	/* PlannerInfo, RelOptInfo */
#include "optimizer/cost.h"		/* set_rel_width() */
#include "optimizer/optimizer.h"	/* cpu_tuple_cost */
#include "optimizer/pathnode.h" /* Path, pathnode_walker() */
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "parser/parse_expr.h"	/* exprType() */
#include "parser/parse_oper.h"
#include "utils/catcache.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "cdb/cdbdef.h"			/* CdbSwap() */
#include "cdb/cdbhash.h"
#include "cdb/cdbpath.h"		/* me */
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"

#include "port/pg_bitutils.h"

typedef struct
{
	CdbPathLocus locus;
	CdbPathLocus move_to;
	double		bytes;
	Path	   *path;
	List       *pathkeys;
	bool		ok_to_replicate;
	bool		require_existing_order;
	bool		has_wts;		/* Does the rel have WorkTableScan? */
	bool		isouter;		/* Is at outer table side? */
} CdbpathMfjRel;

/*
 * We introduced execute on initplan option for function at
 * https://github.com/greenplum-db/gpdb/pull/9542, which introduced
 * a new location option for function: EXECUTE ON INITPLAN and run
 * the f() on initplan.
 *
 * But if f() itself is in initplan, this execution method will cause
 * problems. Therefore, the variable allow_append_initplan_for_function_scan
 * is introduced to control this optimization
 */
static bool allow_append_initplan_for_function_scan = true;

static bool try_redistribute(PlannerInfo *root, CdbpathMfjRel *g,
							 CdbpathMfjRel *o, List *redistribution_clauses, bool parallel_aware);

static SplitUpdatePath *make_splitupdate_path(PlannerInfo *root, Path *subpath, Index rti);

static bool can_elide_explicit_motion(PlannerInfo *root, Index rti, Path *subpath, GpPolicy *policy);
/*
 * cdbpath_cost_motion
 *    Fills in the cost estimate fields in a MotionPath node.
 */
void
cdbpath_cost_motion(PlannerInfo *root, CdbMotionPath *motionpath)
{
	Path	   *subpath = motionpath->subpath;
	Cost		cost_per_row;
	Cost		motioncost;
	double		recvrows;
	double		sendrows;
	double		send_segments = 1;
	double		recv_segments = 1;
	double		total_rows;

	CdbPathLocus sublocus = subpath->locus;
	CdbPathLocus motionlocus = motionpath->path.locus;

	int mot_parallel = motionlocus.parallel_workers;
	int sub_parallel = sublocus.parallel_workers;

	if (CdbPathLocus_IsPartitioned(motionlocus))
	{
		recv_segments = CdbPathLocus_NumSegments(motionlocus);
		if (mot_parallel > 0)
			recv_segments *= mot_parallel;
	}
	else if (mot_parallel > 0 && CdbPathLocus_IsReplicatedWorkers(motionlocus))
		recv_segments *= mot_parallel;

	if (CdbPathLocus_IsPartitioned(sublocus))
	{
		send_segments = CdbPathLocus_NumSegments(sublocus);
		if (sub_parallel > 0)
			send_segments *= sub_parallel;
	}
	else if (sub_parallel > 0 && CdbPathLocus_IsReplicatedWorkers(sublocus))
		send_segments *= sub_parallel;

	/*
	 * Estimate the total number of rows being sent.
	 * The base estimate is computed by multiplying the subpath's rows with
	 * the number of sending segments. But in some cases, that leads to too
	 * large estimates, if the subpath's estimate was "clamped" to 1 row. The
	 * typical example is a single-row select like "SELECT * FROM table WHERE
	 * key = 123. The Scan on the table returns only one row, on one segment,
	 * and the estimate on the Scan node is 1 row. If you have e.g. 3
	 * segments, and we just multiplied the subpath's row estimate by 3, we
	 * would estimate that the Gather returns 3 rows, even though there is
	 * only one matching row in the table. Using the 'rows' estimate on the
	 * RelOptInfo is more accurate in such cases. To correct that, if the
	 * subpath's estimate is 1 row, but the underlying relation's estimate is
	 * smaller, use the underlying relation's estimate.
	 *
	 * We don't always use the relation's estimate, because there might be
	 * nodes like ProjectSet or Limit in the subpath, in which case the
	 * subpath's estimate is more accurate. Also, the relation might not have
	 * a valid 'rows' estimate; upper rels, for example, do not. So check for
	 * that too.
	 */
	total_rows = subpath->rows * send_segments;
	if (subpath->rows == 1.0 &&
		motionpath->path.parent->rows > 0 &&
		motionpath->path.parent->rows < total_rows)
	{
		/* use the RelOptInfo's estimate */
		total_rows = motionpath->path.parent->rows;
	}

	motionpath->path.rows = clamp_row_est(total_rows / recv_segments);

	cost_per_row = (gp_motion_cost_per_row > 0.0)
		? gp_motion_cost_per_row
		: 2.0 * cpu_tuple_cost;

	sendrows = subpath->rows;
	recvrows = motionpath->path.rows;
	motioncost = cost_per_row * 0.5 * (sendrows + recvrows);
	/*
	 * CBDB_PARALLEL_FIXME:
	 * Motioncost may be higher than sendrows + recvrows.
	 * ex: Broadcast Motion 3:6 
	 * Broadcast to prallel workers, each worker's has a rel's all rows(recvrows),
	 * but the transfered cost will double as we will broadcast to 6 workers.
	 */
	if(CdbPathLocus_IsReplicated(motionlocus) && mot_parallel > 0)
		motioncost *= mot_parallel;

	motionpath->path.total_cost = motioncost + subpath->total_cost;
	motionpath->path.startup_cost = subpath->startup_cost;
	motionpath->path.memory = subpath->memory;
}								/* cdbpath_cost_motion */


/*
 * cdbpath_create_motion_path
 *    Returns a Path that delivers the subpath result to the
 *    given locus, or NULL if it can't be done.
 *
 *    'pathkeys' must specify an ordering equal to or weaker than the
 *    subpath's existing ordering.
 *
 *    If no motion is needed, the caller's subpath is returned unchanged.
 *    Else if require_existing_order is true, NULL is returned if the
 *      motion would not preserve an ordering at least as strong as the
 *      specified ordering; also NULL is returned if pathkeys is NIL
 *      meaning the caller is just checking and doesn't want to add motion.
 *    Else a CdbMotionPath is returned having either the specified pathkeys
 *      (if given and the motion uses Merge Receive), or the pathkeys
 *      of the original subpath (if the motion is order-preserving), or no
 *      pathkeys otherwise (the usual case).
 */
Path *
cdbpath_create_motion_path(PlannerInfo *root,
						   Path *subpath,
						   List *pathkeys,
						   bool require_existing_order,
						   CdbPathLocus locus)
{
	CdbMotionPath *pathnode;

	Assert(cdbpathlocus_is_valid(locus) &&
		   cdbpathlocus_is_valid(subpath->locus));

	/*
	 * ISTM, subpath of ReplicatedWorkers only happened if general join with broadcast.
	 * And that only happened if we're doing some updating. Such as:
	 * `explain update rt3 set b = rt2.b from rt2 where rt3.b = rt2.b;`
	 * where rt3 and rt2 should have different numsegments.
	 * However, we don't support parallel update yet, so it will never happen.
	 */
	Assert(!CdbPathLocus_IsReplicatedWorkers(subpath->locus));

	/*
	 * Motion is to change path's locus, if target locus is the
	 * same as the subpath's, there is no need to add motion.
	 */
	if (cdbpathlocus_equal(subpath->locus, locus))
		return subpath;

	/* Moving subpath output to a single executor process (qDisp or qExec)? */
	if (CdbPathLocus_IsOuterQuery(locus))
	{
		/* Outer -> Outer is a no-op */
		if (CdbPathLocus_IsOuterQuery(subpath->locus))
		{
			return subpath;
		}

		if (CdbPathLocus_IsGeneral(subpath->locus))
		{
			/* XXX: this is a bit bogus. We just change the subpath's locus. */
			subpath->locus = locus;
			return subpath;
		}

		if (CdbPathLocus_IsEntry(subpath->locus) ||
			CdbPathLocus_IsSingleQE(subpath->locus))
		{
			/*
			 * XXX: this is a bit bogus. We just change the subpath's locus.
			 *
			 * This is also bogus, because the outer query might need to run
			 * in segments.
			 */
			subpath->locus = locus;
			return subpath;
		}
	}
	else if (CdbPathLocus_IsBottleneck(locus))
	{
		/* entry-->entry?  No motion needed. */
		if (CdbPathLocus_IsEntry(subpath->locus) &&
			CdbPathLocus_IsEntry(locus))
		{
			return subpath;
		}
		/* singleQE-->singleQE?  No motion needed. */
		if (CdbPathLocus_IsSingleQE(subpath->locus) &&
			CdbPathLocus_IsSingleQE(locus))
		{
			subpath->locus.numsegments = CdbPathLocus_CommonSegments(subpath->locus, locus);
			return subpath;
		}

		/* entry-->singleQE?  Don't move.  Slice's QE will run on entry db. */
		if (CdbPathLocus_IsEntry(subpath->locus))
		{
			return subpath;
		}

		/* outerquery-->entry?  No motion needed. */
		if (CdbPathLocus_IsOuterQuery(subpath->locus) &&
			CdbPathLocus_IsEntry(locus))
		{
			return subpath;
		}

		/* singleQE-->entry?  Don't move.  Slice's QE will run on entry db. */
		if (CdbPathLocus_IsSingleQE(subpath->locus))
		{
			/*
			 * If the subpath requires parameters, we cannot generate Motion atop of it.
			 */
			if (!bms_is_empty(PATH_REQ_OUTER(subpath)))
				return NULL;

			/*
			 * Create CdbMotionPath node to indicate that the slice must be
			 * dispatched to a singleton gang running on the entry db.  We
			 * merely use this node to note that the path has 'Entry' locus;
			 * no corresponding Motion node will be created in the Plan tree.
			 */
			Assert(CdbPathLocus_IsEntry(locus));

			pathnode = makeNode(CdbMotionPath);
			pathnode->path.pathtype = T_Motion;
			pathnode->path.parent = subpath->parent;
			/* Motion doesn't project, so use source path's pathtarget */
			pathnode->path.pathtarget = subpath->pathtarget;
			pathnode->path.locus = locus;
			pathnode->path.rows = subpath->rows;

			/* GPDB_96_MERGE_FIXME: When is a Motion path parallel-safe? I tried
			 * setting this to 'false' initially, to play it safe, but somehow
			 * the Paths with motions ended up in gather plans anyway, and tripped
			 * assertion failures.
			 */
			pathnode->path.parallel_aware = false;
			pathnode->path.parallel_safe = subpath->parallel_safe;
			pathnode->path.parallel_workers = locus.parallel_workers;
			pathnode->path.pathkeys = pathkeys;

			pathnode->subpath = subpath;

			/* Costs, etc, are same as subpath. */
			pathnode->path.startup_cost = subpath->total_cost;
			pathnode->path.total_cost = subpath->total_cost;
			pathnode->path.memory = subpath->memory;
			pathnode->path.motionHazard = subpath->motionHazard;
			pathnode->path.barrierHazard = subpath->barrierHazard;

			/* Motion nodes are never rescannable. */
			pathnode->path.rescannable = false;
			return (Path *) pathnode;
		}

		if (CdbPathLocus_IsSegmentGeneral(subpath->locus) ||
			CdbPathLocus_IsSegmentGeneralWorkers(subpath->locus) ||
			CdbPathLocus_IsReplicated(subpath->locus))
		{
			/*
			 * If the subpath requires parameters, we cannot generate Motion atop of it.
			 */
			if (!bms_is_empty(PATH_REQ_OUTER(subpath)))
				return NULL;

			/*
			 * Data is only available on segments, to distingush it with
			 * CdbLocusType_General, adding a motion to indicated this
			 * slice must be executed on a singleton gang.
			 *
			 * This motion may be redundant for segmentGeneral --> singleQE
			 * if the singleQE is not promoted to executed on qDisp in the
			 * end, so in cdbllize_fix_outer_query_motions(), we will omit it.
			 */
			pathnode = makeNode(CdbMotionPath);
			pathnode->path.pathtype = T_Motion;
			pathnode->path.parent = subpath->parent;
			/* Motion doesn't project, so use source path's pathtarget */
			pathnode->path.pathtarget = subpath->pathtarget;
			pathnode->path.locus = locus;
			pathnode->path.rows = subpath->rows;
			pathnode->path.pathkeys = pathkeys;

			/* GPDB_96_MERGE_FIXME: When is a Motion path parallel-safe? I tried
			 * setting this to 'false' initially, to play it safe, but somehow
			 * the Paths with motions ended up in gather plans anyway, and tripped
			 * assertion failures.
			 */
			pathnode->path.parallel_aware = false;
			pathnode->path.parallel_safe = subpath->parallel_safe;
			pathnode->path.parallel_workers = locus.parallel_workers;

			pathnode->subpath = subpath;

			/* Costs, etc, are same as subpath. */
			pathnode->path.startup_cost = subpath->total_cost;
			pathnode->path.total_cost = subpath->total_cost;
			pathnode->path.memory = subpath->memory;
			pathnode->path.motionHazard = subpath->motionHazard;
			pathnode->path.barrierHazard = subpath->barrierHazard;

			/* Motion nodes are never rescannable. */
			pathnode->path.rescannable = false;
			return (Path *) pathnode;
		}

		/* No motion needed if subpath can run anywhere giving same output. */
		if (CdbPathLocus_IsGeneral(subpath->locus))
		{
			/*
			 * general-->(entry|singleqe), no motion is needed, can run
			 * directly on any of the common segments
			 */
			return subpath;
		}

		/* Fail if caller refuses motion. */
		if (require_existing_order &&
			!pathkeys)
			return NULL;

		/*
		 * Must be partitioned-->singleton. If caller gave pathkeys, they'll
		 * be used for Merge Receive. If no pathkeys, Union Receive will
		 * arbitrarily interleave the rows from the subpath partitions in no
		 * special order.
		 */
		if (!CdbPathLocus_IsPartitioned(subpath->locus))
			goto invalid_motion_request;
	}

	/* Output from a single process to be distributed over a gang? */
	else if (CdbPathLocus_IsBottleneck(subpath->locus))
	{
		/* Must be bottleneck-->partitioned or bottleneck-->replicated */
		if (!CdbPathLocus_IsPartitioned(locus) &&
			!CdbPathLocus_IsReplicated(locus))
			goto invalid_motion_request;

		/* Fail if caller disallows motion. */
		if (require_existing_order &&
			!pathkeys)
			return NULL;

		/* Each qExec receives a subset of the rows, with ordering preserved. */
		pathkeys = subpath->pathkeys;
	}

	/* Redistributing partitioned subpath output from one gang to another? */
	else if (CdbPathLocus_IsPartitioned(subpath->locus))
	{
		/* partitioned-->partitioned? */
		if (CdbPathLocus_IsPartitioned(locus))
		{
			/* No motion if subpath partitioning matches caller's request. */
			if (cdbpathlocus_equal(subpath->locus, locus))
				return subpath;
		}

		/* Must be partitioned-->replicated */
		else if (!CdbPathLocus_IsReplicated(locus) && !CdbPathLocus_IsHashedWorkers(locus) && !CdbPathLocus_IsReplicatedWorkers(locus))
			goto invalid_motion_request;

		/* Fail if caller insists on ordered result or no motion. */
		if (require_existing_order)
			return NULL;

		/*
		 * Output streams lose any ordering they had. Only a qDisp or
		 * singleton qExec can merge sorted streams (for now).
		 */
		pathkeys = NIL;
	}

	/* If subplan uses no tables, it can run on qDisp or a singleton qExec. */
	else if (CdbPathLocus_IsGeneral(subpath->locus))
	{
		/*
		 * Parallel replicating is now only happening if both sides are not general.
		 */
		Assert(!CdbPathLocus_IsReplicatedWorkers(locus));
		/*
		 * No motion needed if general-->general or general-->replicated or
		 * general-->segmentGeneral
		 */
		if (CdbPathLocus_IsGeneral(locus) ||
			CdbPathLocus_IsReplicated(locus) ||
			CdbPathLocus_IsSegmentGeneral(locus))
		{
			return subpath;
		}

		if (CdbPathLocus_IsSegmentGeneralWorkers(subpath->locus))
			goto invalid_motion_request;

		/* Must be general-->partitioned. */
		if (!CdbPathLocus_IsPartitioned(locus))
			goto invalid_motion_request;

		/* Fail if caller wants no motion. */
		if (require_existing_order &&
			!pathkeys)
			return NULL;

		/* Since the motion is 1-to-many, the rows remain in the same order. */
		pathkeys = subpath->pathkeys;
	}

	/* Does subpath produce same multiset of rows on every qExec of its gang? */
	else if (CdbPathLocus_IsReplicated(subpath->locus))
	{
		/* No-op if replicated-->replicated. */
		if (CdbPathLocus_IsReplicated(locus))
		{
			Assert(CdbPathLocus_NumSegments(locus) <=
				   CdbPathLocus_NumSegments(subpath->locus));
			subpath->locus.numsegments = CdbPathLocus_NumSegments(locus);
			return subpath;
		}

		/* Other destinations aren't used or supported at present. */
		goto invalid_motion_request;
	}

	/* Most motions from SegmentGeneral (replicated table) are disallowed */
	else if (CdbPathLocus_IsSegmentGeneral(subpath->locus) || CdbPathLocus_IsSegmentGeneralWorkers(subpath->locus))
	{
		/*
		 * The only allowed case is a SegmentGeneral to Hashed motion,
		 * and SegmentGeneral's numsegments is smaller than Hashed's.
		 * In such a case we redistribute SegmentGeneral to Hashed.
		 *
		 * FIXME: HashedOJ?
		 */
		if (CdbPathLocus_IsPartitioned(locus))
		{
			pathkeys = subpath->pathkeys;
		}
		else if (CdbPathLocus_IsReplicated(locus))
		{
			if (CdbPathLocus_NumSegments(locus) <= CdbPathLocus_NumSegments(subpath->locus))
			{
				subpath->locus.numsegments = CdbPathLocus_NumSegments(locus);
				return subpath;
			}
			else
			{
				pathkeys = subpath->pathkeys;
			}
		}
		else if (CdbPathLocus_IsSegmentGeneral(locus))
		{
			subpath->locus.numsegments = Min(subpath->locus.numsegments, locus.numsegments);
			return subpath;
		}
		else
			goto invalid_motion_request;
	}
	else
		goto invalid_motion_request;

	/* Don't materialize before motion. */
	if (IsA(subpath, MaterialPath))
		subpath = ((MaterialPath *) subpath)->subpath;

	/*
	 * MPP-3300: materialize *before* motion can never help us, motion pushes
	 * data. other nodes pull. We relieve motion deadlocks by adding
	 * materialize nodes on top of motion nodes
	 */


    if(IsA(subpath,SubqueryScanPath)
       && CdbPathLocus_IsBottleneck(locus)
       && !subpath->pathkeys
       && ((SubqueryScanPath *)subpath)->subpath->pathkeys)
    {
        /*
         * In gpdb, when there is a Gather Motion on top of a SubqueryScan,
         * it is hard to keep the sort order information. The subquery's
         * path key cannot be pulled up, if the parent query doesn't have
         * an equivalence class corresponding to the subquery's sort order.
         *
         * e.g. create a view with an ORDER BY:
         *
         * CREATE VIEW v AS SELECT va, vn FROM sourcetable ORDER BY vn;
         *
         * and query it:
         *
         * SELECT va FROM v_sourcetable;
         *
         * So, push down the Gather Motion if the SubqueryScan dose not
         * have pathkey but the SubqueryScan's subpath does.
         *
         */
        SubqueryScanPath *subqueryScanPath = (SubqueryScanPath *)subpath;
        SubqueryScanPath *newSubqueryScanPath = NULL;
        Path *motionPath = NULL;

        subpath = subqueryScanPath->subpath;

        motionPath = cdbpath_create_motion_path(root,
                                                subpath,
                                                subpath->pathkeys,
                                                true,
                                                locus);

        newSubqueryScanPath = create_subqueryscan_path(root,
                                                       subqueryScanPath->path.parent,
                                                       motionPath,
                                                       subqueryScanPath->path.pathkeys,
                                                       locus,
                                                       subqueryScanPath->required_outer);

	/* apply final path target */
        newSubqueryScanPath = (SubqueryScanPath *)apply_projection_to_path(root,
                                                                           subqueryScanPath->path.parent,
                                                                           (Path *) newSubqueryScanPath,
                                                                           subqueryScanPath->path.pathtarget);

        return (Path *) newSubqueryScanPath;
    }

	/*
	 * If the subpath requires parameters, we cannot generate Motion atop of it.
	 */
	if (!bms_is_empty(PATH_REQ_OUTER(subpath)))
		return NULL;

	/* Create CdbMotionPath node. */
	pathnode = makeNode(CdbMotionPath);
	pathnode->path.pathtype = T_Motion;
	pathnode->path.parent = subpath->parent;
	/* Motion doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	pathnode->path.locus = locus;
	pathnode->path.rows = subpath->rows;
	pathnode->path.pathkeys = pathkeys;

	/* GPDB_96_MERGE_FIXME: When is a Motion path parallel-safe? I tried
	 * setting this to 'false' initially, to play it safe, but somehow
	 * the Paths with motions ended up in gather plans anyway, and tripped
	 * assertion failures.
	 */
	pathnode->path.parallel_aware = false;
	/*
	 * CBDB_PARALLEL_FIXME:
	 * We once set parallel_safe by locus type, but almost all locus are
	 * parallel safe nowadays.
	 * In principle, we should set parallel_safe = true if we are in a parallel join.
	 * TODO: Set parallel_safe to true for all locus.
	 */
	pathnode->path.parallel_safe = (locus.parallel_workers > 0 ||
									CdbPathLocus_IsHashedWorkers(locus) ||
									CdbPathLocus_IsSingleQE(locus) ||
									CdbPathLocus_IsEntry(locus) ||
									CdbPathLocus_IsReplicatedWorkers(locus) ||
									CdbPathLocus_IsReplicated(locus) || /* CTAS replicated table */
									CdbPathLocus_IsHashed(locus));
	if (!subpath->parallel_safe)
		pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = locus.parallel_workers;

	pathnode->subpath = subpath;
	pathnode->is_explicit_motion = false;

	/* Cost of motion */
	cdbpath_cost_motion(root, pathnode);

	/* Tell operators above us that slack may be needed for deadlock safety. */
	pathnode->path.motionHazard = true;
	/*
	 * If parallel workers > 0, which means barrier hazard exits for parallel
	 * hash join.
	 */
	pathnode->path.barrierHazard = (locus.parallel_workers > 0);
	pathnode->path.rescannable = false;

	/*
	 * A motion to bring data to the outer query's locus needs a Material node
	 * on top, to shield the Motion node from rescanning, when the SubPlan
	 * is rescanned.
	 */
	if (CdbPathLocus_IsOuterQuery(locus))
	{
		return (Path *) create_material_path(subpath->parent, &pathnode->path);
	}

	return (Path *) pathnode;

	/* Unexpected source or destination locus. */
invalid_motion_request:
	elog(ERROR, "could not build Motion path");
	return NULL;
}								/* cdbpath_create_motion_path */

Path *
cdbpath_create_explicit_motion_path(PlannerInfo *root,
									Path *subpath,
									CdbPathLocus locus)
{
	CdbMotionPath *pathnode;

	/* Create CdbMotionPath node. */
	pathnode = makeNode(CdbMotionPath);
	pathnode->path.pathtype = T_Motion;
	pathnode->path.parent = subpath->parent;
	/* Motion doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	pathnode->path.locus = locus;
	pathnode->path.rows = subpath->rows;
	pathnode->path.pathkeys = NIL;

	/* GPDB_96_MERGE_FIXME: When is a Motion path parallel-safe? I tried
	 * setting this to 'false' initially, to play it safe, but somehow
	 * the Paths with motions ended up in gather plans anyway, and tripped
	 * assertion failures.
	 */
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = subpath->parallel_workers;

	pathnode->subpath = subpath;
	pathnode->is_explicit_motion = true;

	/* Cost of motion */
	cdbpath_cost_motion(root, pathnode);

	/* Tell operators above us that slack may be needed for deadlock safety. */
	pathnode->path.motionHazard = true;
	pathnode->path.barrierHazard = false;
	pathnode->path.rescannable = false;

	return (Path *) pathnode;
}

Path *
cdbpath_create_broadcast_motion_path(PlannerInfo *root,
									 Path *subpath,
									 int numsegments)
{
	CdbMotionPath *pathnode;

	/* Create CdbMotionPath node. */
	pathnode = makeNode(CdbMotionPath);
	pathnode->path.pathtype = T_Motion;
	pathnode->path.parent = subpath->parent;
	/* Motion doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	CdbPathLocus_MakeReplicated(&pathnode->path.locus, numsegments, subpath->parallel_workers);
	pathnode->path.rows = subpath->rows;
	pathnode->path.pathkeys = NIL;

	/* GPDB_96_MERGE_FIXME: When is a Motion path parallel-safe? I tried
	 * setting this to 'false' initially, to play it safe, but somehow
	 * the Paths with motions ended up in gather plans anyway, and tripped
	 * assertion failures.
	 */
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = subpath->parallel_workers;

	pathnode->subpath = subpath;
	pathnode->is_explicit_motion = false;

	/* Cost of motion */
	cdbpath_cost_motion(root, pathnode);

	/* Tell operators above us that slack may be needed for deadlock safety. */
	pathnode->path.motionHazard = true;
	pathnode->path.barrierHazard = false;
	pathnode->path.rescannable = false;

	return (Path *) pathnode;
}

/*
 */
static CdbMotionPath *
make_motion_path(PlannerInfo *root, Path *subpath,
				 CdbPathLocus locus,
				 bool is_explicit_motion,
				 GpPolicy *policy)
{
	CdbMotionPath *pathnode;

	/* Create CdbMotionPath node. */
	pathnode = makeNode(CdbMotionPath);
	pathnode->path.pathtype = T_Motion;
	pathnode->path.parent = subpath->parent;
	/* Motion doesn't project, so use source path's pathtarget */
	pathnode->path.pathtarget = subpath->pathtarget;
	pathnode->path.locus = locus;
	pathnode->path.rows = subpath->rows;
	pathnode->path.pathkeys = NIL;

	/* GPDB_96_MERGE_FIXME: When is a Motion path parallel-safe? I tried
	 * setting this to 'false' initially, to play it safe, but somehow
	 * the Paths with motions ended up in gather plans anyway, and tripped
	 * assertion failures.
	 */
	pathnode->path.parallel_aware = false;
	pathnode->path.parallel_safe = false;
	pathnode->path.parallel_workers = subpath->parallel_workers;

	pathnode->subpath = subpath;

	pathnode->is_explicit_motion = is_explicit_motion;
	pathnode->policy = policy;

	/* Cost of motion */
	cdbpath_cost_motion(root, pathnode);

	/* Tell operators above us that slack may be needed for deadlock safety. */
	pathnode->path.motionHazard = true;
	pathnode->path.barrierHazard = false;
	pathnode->path.rescannable = false;

	return pathnode;
}

/*
 * cdbpath_match_preds_to_partkey_tail
 *
 * Returns true if all of the locus's partitioning key expressions are
 * found as comparands of equijoin predicates in the mergeclause_list.
 *
 * NB: for mergeclause_list and pathkey structure assumptions, see:
 *          select_mergejoin_clauses() in joinpath.c
 *          find_mergeclauses_for_pathkeys() in pathkeys.c
 */

typedef struct
{
	PlannerInfo *root;
	List	   *mergeclause_list;
	Path       *path;
	CdbPathLocus locus;
    CdbPathLocus otherlocus;
	CdbPathLocus *colocus;
	bool		colocus_eq_locus;
} CdbpathMatchPredsContext;


/*
 * A helper function to create a DistributionKey for an EquivalenceClass.
 */
static DistributionKey *
makeDistributionKeyForEC(EquivalenceClass *eclass, Oid opfamily)
{
	DistributionKey *dk = makeNode(DistributionKey);

	Assert(OidIsValid(opfamily));

	dk->dk_eclasses = list_make1(eclass);
	dk->dk_opfamily = opfamily;

	return dk;
}

/*
 * cdbpath_eclass_constant_is_hashable
 *
 * Iterates through a list of equivalence class members and determines if
 * expression in pseudoconstant is hashable under the given hash opfamily.
 *
 * If there are multiple constants in the equivalence class, it is sufficient
 * that one of them is usable.
 */
static bool
cdbpath_eclass_constant_is_hashable(EquivalenceClass *ec, Oid hashOpFamily)
{
	ListCell   *j;

	foreach(j, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(j);

		if (!em->em_is_const)
			continue;

		if (get_opfamily_member(hashOpFamily, em->em_datatype, em->em_datatype,
								HTEqualStrategyNumber))
			return true;
	}

	return false;
}

static bool
cdbpath_match_preds_to_distkey_tail(CdbpathMatchPredsContext *ctx,
									List *list, ListCell *distkeycell, bool parallel_aware)
{
	DistributionKey *distkey = (DistributionKey *) lfirst(distkeycell);
	DistributionKey *codistkey;
	ListCell   *cell;
	ListCell   *rcell;

	Assert(CdbPathLocus_IsHashed(ctx->locus) ||
		   CdbPathLocus_IsHashedWorkers(ctx->locus) ||
		   CdbPathLocus_IsHashedOJ(ctx->locus));

	/*
	 * Try ro redistributed one to match another.
	 * non-parallel_aware
	 * HashedWorkers can only work with replica, can't redistributed one to match
	 */
	if (!parallel_aware && CdbPathLocus_IsHashedWorkers(ctx->locus))
		return false;

	/*----------------
	 * Is there a "<distkey item> = <constant expr>" predicate?
	 *
	 * If table T is distributed on cols (C,D,E) and query contains preds
	 *		T.C = U.A AND T.D = <constant expr> AND T.E = U.B
	 * then we would like to report a match and return the colocus
	 * 		(U.A, <constant expr>, U.B)
	 * so the caller can join T and U by redistributing only U.
	 * (Note that "T.D = <constant expr>" won't be in the mergeclause_list
	 * because it isn't a join pred.)
	 *----------------
	 */
	codistkey = NULL;

	foreach(cell, distkey->dk_eclasses)
	{
		EquivalenceClass *ec = (EquivalenceClass *) lfirst(cell);

		if (CdbEquivClassIsConstant(ec) &&
			cdbpath_eclass_constant_is_hashable(ec, distkey->dk_opfamily))
		{
			codistkey = distkey;
			break;
		}
	}

	/* Look for an equijoin comparison to the distkey item. */
	if (!codistkey)
	{
		foreach(rcell, ctx->mergeclause_list)
		{
			EquivalenceClass *a_ec; /* Corresponding to ctx->path. */
			EquivalenceClass *b_ec;
			ListCell   *i;
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(rcell);

			update_mergeclause_eclasses(ctx->root, rinfo);

			if (bms_is_subset(rinfo->right_relids, ctx->path->parent->relids))
			{
				a_ec = rinfo->right_ec;
				b_ec = rinfo->left_ec;
			}
			else
			{
				a_ec = rinfo->left_ec;
				b_ec = rinfo->right_ec;
				Assert(bms_is_subset(rinfo->left_relids, ctx->path->parent->relids));
			}

			foreach(i, distkey->dk_eclasses)
			{
				EquivalenceClass *dk_eclass = (EquivalenceClass *) lfirst(i);

				if (dk_eclass == a_ec)
					codistkey = makeDistributionKeyForEC(b_ec, distkey->dk_opfamily); /* break earlier? */
			}

			if (codistkey)
				break;
		}
	}

	/* Fail if didn't find a match for this distkey item. */
	if (!codistkey)
		return false;

	/* Might need to build co-locus if locus is outer join source or result. */
	if (codistkey != lfirst(distkeycell))
		ctx->colocus_eq_locus = false;

	/* Match remaining partkey items. */
	distkeycell = lnext(list, distkeycell);
	if (distkeycell)
	{
		if (!cdbpath_match_preds_to_distkey_tail(ctx, list, distkeycell, parallel_aware))
			return false;
	}

	/* Success!  Matched all items.  Return co-locus if requested. */
	if (ctx->colocus)
	{
		if (ctx->colocus_eq_locus)
			*ctx->colocus = ctx->locus;
		else if (!distkeycell)
			CdbPathLocus_MakeHashed(ctx->colocus, list_make1(codistkey),
									CdbPathLocus_NumSegments(ctx->locus),
									ctx->locus.parallel_workers);
		else
		{
			ctx->colocus->distkey = lcons(codistkey, ctx->colocus->distkey);
			Assert(cdbpathlocus_is_valid(*ctx->colocus));
		}
	}
	return true;
}								/* cdbpath_match_preds_to_partkey_tail */



/*
 * cdbpath_match_preds_to_partkey
 *
 * Returns true if an equijoin predicate is found in the mergeclause_list
 * for each item of the locus's partitioning key.
 *
 * (Also, a partkey item that is equal to a constant is treated as a match.)
 *
 * Readers may refer also to these related functions:
 *          select_mergejoin_clauses() in joinpath.c
 *          find_mergeclauses_for_pathkeys() in pathkeys.c
 */
static bool
cdbpath_match_preds_to_distkey(PlannerInfo *root,
							   List *mergeclause_list,
							   Path *path,
							   CdbPathLocus locus,
							   CdbPathLocus otherlocus,
							   bool parallel_aware,
							   CdbPathLocus *colocus)	/* OUT */
{
	CdbpathMatchPredsContext ctx;

	if (!CdbPathLocus_IsHashed(locus) &&
		!CdbPathLocus_IsHashedOJ(locus) &&
		!CdbPathLocus_IsHashedWorkers(locus))
		return false;

	/*
	 * Don't bother to redistribute to non-parallel locus if parallel_aware is true.
	 * We should already consider non-parallel join of the same two path before.
	 */
	if (locus.parallel_workers == 0 && parallel_aware)
		return false;

	if (!parallel_aware && CdbPathLocus_IsHashedWorkers(locus))
		return false;

	Assert(cdbpathlocus_is_valid(locus));

	ctx.root = root;
	ctx.mergeclause_list = mergeclause_list;
	ctx.path = path;
	ctx.locus = locus;
	ctx.otherlocus = otherlocus;
	ctx.colocus = colocus;
	ctx.colocus_eq_locus = true;

	return cdbpath_match_preds_to_distkey_tail(&ctx, locus.distkey, list_head(locus.distkey), parallel_aware);
}


/*
 * cdbpath_match_preds_to_both_distkeys
 *
 * Returns true if the mergeclause_list contains equijoin
 * predicates between each item of the outer_locus distkey and
 * the corresponding item of the inner_locus distkey.
 *
 * Readers may refer also to these related functions:
 *          select_mergejoin_clauses() in joinpath.c
 *          find_mergeclauses_for_pathkeys() in pathkeys.c
 */
static bool
cdbpath_match_preds_to_both_distkeys(PlannerInfo *root,
									 List *mergeclause_list,
									 CdbPathLocus outer_locus,
									 CdbPathLocus inner_locus,
									 bool parallel_aware)
{
	ListCell   *outercell;
	ListCell   *innercell;
	List	   *outer_distkey;
	List	   *inner_distkey;

	if (!mergeclause_list ||
		CdbPathLocus_NumSegments(outer_locus) != CdbPathLocus_NumSegments(inner_locus) ||
		outer_locus.distkey == NIL || inner_locus.distkey == NIL ||
		CdbPathLocus_NumParallelWorkers(outer_locus) != CdbPathLocus_NumParallelWorkers(inner_locus) ||
		list_length(outer_locus.distkey) != list_length(inner_locus.distkey))
		return false;

	Assert(CdbPathLocus_IsHashed(outer_locus) ||
		   CdbPathLocus_IsHashedWorkers(outer_locus) ||
		   CdbPathLocus_IsHashedOJ(outer_locus));
	Assert(CdbPathLocus_IsHashed(inner_locus) ||
		   CdbPathLocus_IsHashedWorkers(inner_locus) ||
		   CdbPathLocus_IsHashedOJ(inner_locus));

	if (!parallel_aware && (CdbPathLocus_IsHashedWorkers(outer_locus) || CdbPathLocus_IsHashedWorkers(inner_locus)))
		return false;

	outer_distkey = outer_locus.distkey;
	inner_distkey = inner_locus.distkey;

	forboth(outercell, outer_distkey, innercell, inner_distkey)
	{
		DistributionKey *outer_dk = (DistributionKey *) lfirst(outercell);
		DistributionKey *inner_dk = (DistributionKey *) lfirst(innercell);
		ListCell   *rcell;

		if (outer_dk->dk_opfamily != inner_dk->dk_opfamily)
			return false;	/* incompatible hashing scheme */

		foreach(rcell, mergeclause_list)
		{
			bool		not_found = false;
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(rcell);
			ListCell   *i;

			update_mergeclause_eclasses(root, rinfo);

			/* Skip predicate if neither side matches outer distkey item. */
			foreach(i, outer_dk->dk_eclasses)
			{
				EquivalenceClass *outer_ec = (EquivalenceClass *) lfirst(i);

				if (outer_ec != rinfo->left_ec && outer_ec != rinfo->right_ec)
				{
					not_found = true;
					break;
				}
			}
			if (not_found)
				continue;

			/* Skip predicate if neither side matches inner distkey item. */
			if (inner_dk != outer_dk)
			{
				ListCell   *i;

				foreach(i, inner_dk->dk_eclasses)
				{
					EquivalenceClass *inner_ec = (EquivalenceClass *) lfirst(i);

					if (inner_ec != rinfo->left_ec && inner_ec != rinfo->right_ec)
					{
						not_found = true;
						break;
					}
				}
				if (not_found)
					continue;
			}

			/* Found equijoin between outer distkey item & inner distkey item */
			break;
		}

		/* Fail if didn't find equijoin between this pair of distkey items. */
		if (!rcell)
			return false;
	}
	return true;
}								/* cdbpath_match_preds_to_both_distkeys */

/*
 * cdbpath_distkeys_from_preds
 *
 * Makes a CdbPathLocus for repartitioning, driven by
 * the equijoin predicates in the mergeclause_list (a List of RestrictInfo).
 * Returns true if successful, or false if no usable equijoin predicates.
 *
 * Readers may refer also to these related functions:
 *      select_mergejoin_clauses() in joinpath.c
 *      make_pathkeys_for_mergeclauses() in pathkeys.c
 */
static bool
cdbpath_distkeys_from_preds(PlannerInfo *root,
							List *mergeclause_list,
							Path *a_path,
							int numsegments,
							int parallel_workers,
							bool parallel_aware,
							CdbPathLocus *a_locus,	/* OUT */
							CdbPathLocus *b_locus)	/* OUT */
{
	List	   *a_distkeys = NIL;
	List	   *b_distkeys = NIL;
	ListCell   *rcell;

	foreach(rcell, mergeclause_list)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(rcell);
		Oid			lhs_opno;
		Oid			rhs_opno;
		Oid			opfamily;

		update_mergeclause_eclasses(root, rinfo);

		/*
		 * skip non-hashable keys
		 */
		if (!rinfo->hashjoinoperator)
			continue;

		/*
		 * look up a hash operator family that is compatible for the left and right datatypes
		 * of the hashjoin = operator
		 */
		if (!get_compatible_hash_operators_and_family(rinfo->hashjoinoperator,
													  &lhs_opno, &rhs_opno, &opfamily))
			continue;

		/* Left & right pathkeys are usually the same... */
		if (!b_distkeys && rinfo->left_ec == rinfo->right_ec)
		{
			ListCell   *i;
			bool        found = false;

			foreach(i, a_distkeys)
			{
				DistributionKey *distkey = (DistributionKey *) lfirst(i);
				EquivalenceClass *dk_eclass;

				/*
				 * we only create Hashed DistributionKeys with a single eclass
				 * in this function.
				 */
				Assert(list_length(distkey->dk_eclasses) == 1);
				dk_eclass = (EquivalenceClass *) linitial(distkey->dk_eclasses);

				if (dk_eclass == rinfo->left_ec)
				{
					found = true;
					break;
				}
			}
			if (!found)
			{
				DistributionKey *a_dk = makeDistributionKeyForEC(rinfo->left_ec, opfamily);
				a_distkeys = lappend(a_distkeys, a_dk);
			}
		}

		/* ... except in outer join ON-clause. */
		else
		{
			EquivalenceClass *a_ec;
			EquivalenceClass *b_ec;
			ListCell   *i;
			bool		found = false;

			if (bms_is_subset(rinfo->right_relids, a_path->parent->relids))
			{
				a_ec = rinfo->right_ec;
				b_ec = rinfo->left_ec;
			}
			else
			{
				a_ec = rinfo->left_ec;
				b_ec = rinfo->right_ec;
				Assert(bms_is_subset(rinfo->left_relids, a_path->parent->relids));
			}

			if (!b_ec)
				b_ec = a_ec;

			/*
			 * Convoluted logic to ensure that (a_ec not in a_distkeys) AND
			 * (b_ec not in b_distkeys)
			 */
			found = false;
			foreach(i, a_distkeys)
			{
				DistributionKey *distkey = (DistributionKey *) lfirst(i);
				EquivalenceClass *dk_eclass;

				/*
				 * we only create Hashed DistributionKeys with a single eclass
				 * in this function.
				 */
				Assert(list_length(distkey->dk_eclasses) == 1);
				dk_eclass = (EquivalenceClass *) linitial(distkey->dk_eclasses);

				if (dk_eclass == a_ec)
				{
					found = true;
					break;
				}
			}
			if (!found)
			{
				foreach(i, b_distkeys)
				{
					DistributionKey *distkey = (DistributionKey *) lfirst(i);
					EquivalenceClass *dk_eclass;

					/*
					 * we only create Hashed DistributionKeys with a single eclass
					 * in this function.
					 */
					Assert(list_length(distkey->dk_eclasses) == 1);
					dk_eclass = (EquivalenceClass *) linitial(distkey->dk_eclasses);

					if (dk_eclass == b_ec)
					{
						found = true;
						break;
					}
				}
			}

			if (!found)
			{
				DistributionKey *a_dk = makeDistributionKeyForEC(a_ec, opfamily);
				DistributionKey *b_dk = makeDistributionKeyForEC(b_ec, opfamily);

				a_distkeys = lappend(a_distkeys, a_dk);
				b_distkeys = lappend(b_distkeys, b_dk);
			}
		}

		if (list_length(a_distkeys) >= 20)
			break;
	}

	if (!a_distkeys)
		return false;

	CdbPathLocus_MakeHashed(a_locus, a_distkeys, numsegments, parallel_workers);
	if (b_distkeys)
		CdbPathLocus_MakeHashed(b_locus, b_distkeys, numsegments, parallel_workers);
	else
		*b_locus = *a_locus;
	return true;
}								/* cdbpath_distkeys_from_preds */

/*
 * Add a RowIdExpr to the target list of 'path'. Returns the ID
 * of the generated rowid expression in *rowidexpr_id.
 */
static Path *
add_rowid_to_path(PlannerInfo *root, Path *path, int *rowidexpr_id)
{
	RowIdExpr *rowidexpr;
	PathTarget *newpathtarget;

	/*
	 * 'last_rowidexpr_id' is used to generate a unique ID for the RowIdExpr
	 * node that we generate. It only needs to be unique within this query
	 * plan, and the simplest way to achieve that is to just have a global
	 * counter. (Actually, it isn't really needed at the moment because the
	 * deduplication is always done immediately on top of the join, so two
	 * different RowIdExprs should never appear in the same part of the plan
	 * tree. But it might come handy when debugging, if nothing else.
	 * XXX: If we start to rely on it for something important, consider
	 * overflow behavior more carefully.)
	 */
	static uint32 last_rowidexpr_id = 0;

	rowidexpr = makeNode(RowIdExpr);
	last_rowidexpr_id++;

	*rowidexpr_id = rowidexpr->rowidexpr_id = (int) last_rowidexpr_id;

	newpathtarget = copy_pathtarget(path->pathtarget);
	add_column_to_pathtarget(newpathtarget, (Expr *) rowidexpr, 0);

	return (Path *) create_projection_path_with_quals(root, path->parent,
													  path, newpathtarget,
													  NIL, true);
}

/*
 * cdbpath_motion_for_join
 *
 * Decides where a join should be done.  Adds Motion operators atop
 * the subpaths if needed to deliver their results to the join locus.
 * Returns the join locus if ok, or a null locus otherwise. If
 * jointype is JOIN_SEMI_DEDUP or JOIN_SEMI_DEDUP_REVERSE, this also
 * tacks a RowIdExpr on one side of the join, and *p_rowidexpr_id is
 * set to the ID of that. The caller is expected to uniquefy
 * the result after the join, passing the rowidexpr_id to
 * create_unique_rowid_path().
 *
 * mergeclause_list is a List of RestrictInfo.  Its members are
 * the equijoin predicates between the outer and inner rel.
 * It comes from select_mergejoin_clauses() in joinpath.c.
 */
CdbPathLocus
cdbpath_motion_for_join(PlannerInfo *root,
						JoinType jointype,	/* JOIN_INNER/FULL/LEFT/RIGHT/IN */
						Path **p_outer_path,	/* INOUT */
						Path **p_inner_path,	/* INOUT */
						int *p_rowidexpr_id,	/* OUT */
						List *redistribution_clauses, /* equijoin RestrictInfo list */
						List *restrict_clauses,
						List *outer_pathkeys,
						List *inner_pathkeys,
						bool outer_require_existing_order,
						bool inner_require_existing_order)
{
	CdbpathMfjRel 	outer;
	CdbpathMfjRel 	inner;
	int 			numsegments;
	bool 			join_quals_contain_outer_references;
	ListCell 		*lc;

	*p_rowidexpr_id = 0;

	outer.pathkeys = outer_pathkeys;
	inner.pathkeys = inner_pathkeys;
	outer.path = *p_outer_path;
	inner.path = *p_inner_path;
	outer.locus = outer.path->locus;
	inner.locus = inner.path->locus;
	CdbPathLocus_MakeNull(&outer.move_to);
	CdbPathLocus_MakeNull(&inner.move_to);

	Assert(cdbpathlocus_is_valid(outer.locus));
	Assert(cdbpathlocus_is_valid(inner.locus));

	/* No parallel paths should get here. */
	Assert(outer.locus.parallel_workers == 0);
	Assert(inner.locus.parallel_workers == 0);

	/*
	 * Does the join quals contain references to outer query? If so, we must
	 * evaluate them in the outer query's locus. That means pulling both
	 * inputs to outer locus, and performing the join there.
	 *
	 * XXX: If there are pseudoconstant quals, they will be executed by a
	 * gating Result with a One-Time Filter. In that case, the join's inputs
	 * wouldn't need to be brought to the outer locus. We could execute the
	 * join normally, and bring the result to the outer locus and put the
	 * gating Result above the Motion, instead. But for now, we're not smart
	 * like that.
	 */
	join_quals_contain_outer_references = false;
	foreach(lc, restrict_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (rinfo->contain_outer_query_references)
		{
			join_quals_contain_outer_references = true;
			break;
		}
	}

	outer.has_wts = cdbpath_contains_wts(outer.path);
	inner.has_wts = cdbpath_contains_wts(inner.path);

	/* For now, inner path should not contain WorkTableScan */
	Assert(!inner.has_wts);

	/*
	 * If outer rel contains WorkTableScan and inner rel is hash distributed,
	 * unfortunately we have to pretend that inner is randomly distributed,
	 * otherwise we may end up with redistributing outer rel.
	 */
	if (outer.has_wts && inner.locus.distkey != NIL)
		CdbPathLocus_MakeStrewn(&inner.locus,
								CdbPathLocus_NumSegments(inner.locus), 0);

	/*
	 * Caller can specify an ordering for each source path that is the same as
	 * or weaker than the path's existing ordering. Caller may insist that we
	 * do not add motion that would lose the specified ordering property;
	 * otherwise the given ordering is preferred but not required. A required
	 * NIL ordering means no motion is allowed for that path.
	 */
	outer.require_existing_order = outer_require_existing_order;
	inner.require_existing_order = inner_require_existing_order;

	/*
	 * Don't consider replicating the preserved rel of an outer join, or the
	 * current-query rel of a join between current query and subquery.
	 *
	 * Path that contains WorkTableScan cannot be replicated.
	 */
	/* ok_to_replicate means broadcast */
	outer.ok_to_replicate = !outer.has_wts;
	inner.ok_to_replicate = true;
	switch (jointype)
	{
		case JOIN_INNER:
		case JOIN_UNIQUE_OUTER:
		case JOIN_UNIQUE_INNER:
			break;
		case JOIN_SEMI:
		case JOIN_ANTI:
		case JOIN_LEFT:
		case JOIN_LASJ_NOTIN:
			outer.ok_to_replicate = false;
			break;
		case JOIN_RIGHT:
			inner.ok_to_replicate = false;
			break;
		case JOIN_FULL:
			outer.ok_to_replicate = false;
			inner.ok_to_replicate = false;
			break;

		case JOIN_DEDUP_SEMI:

			/*
			 * In this plan type, we generate a unique row ID on the outer
			 * side of the join, perform the join, possibly broadcasting the
			 * outer side, and remove duplicates after the join, so that only
			 * one row for each input outer row remains.
			 *
			 * If the outer input is General or SegmentGeneral, it's available
			 * in all the segments, but we cannot reliably generate a row ID
			 * to distinguish each logical row in that case. So force the
			 * input to a single node first in that case.
			 *
			 * In previous Cloudberry versions, we assumed that we can generate
			 * a unique row ID for General paths, by generating the same
			 * sequence of numbers on each segment. That works as long as the
			 * rows are in the same order on each segment, but it seemed like
			 * a risky assumption. And it didn't work on SegmentGeneral paths
			 * (i.e. replicated tables) anyway.
			 */
			if (!CdbPathLocus_IsPartitioned(inner.locus))
				goto fail;

			if (CdbPathLocus_IsPartitioned(outer.locus) ||
				CdbPathLocus_IsBottleneck(outer.locus))
			{
				/* ok */
			}
			else if (CdbPathLocus_IsGeneral(outer.locus))
			{
				CdbPathLocus_MakeSingleQE(&outer.locus,
										  CdbPathLocus_NumSegments(inner.locus));
				outer.path->locus = outer.locus;
			}
			else if (CdbPathLocus_IsSegmentGeneral(outer.locus))
			{
				CdbPathLocus_MakeSingleQE(&outer.locus,
										  CdbPathLocus_CommonSegments(inner.locus,
																	  outer.locus));
				outer.path->locus = outer.locus;
			}
			else
				goto fail;
			inner.ok_to_replicate = false;
			outer.path = add_rowid_to_path(root, outer.path, p_rowidexpr_id);
			*p_outer_path = outer.path;
			break;

		case JOIN_DEDUP_SEMI_REVERSE:
			/* same as JOIN_DEDUP_SEMI, but with inner and outer reversed */
			if (!CdbPathLocus_IsPartitioned(outer.locus))
				goto fail;
			if (CdbPathLocus_IsPartitioned(inner.locus) ||
				CdbPathLocus_IsBottleneck(inner.locus))
			{
				/* ok */
			}
			else if (CdbPathLocus_IsGeneral(inner.locus))
			{
				CdbPathLocus_MakeSingleQE(&inner.locus,
										  CdbPathLocus_NumSegments(outer.locus));
				inner.path->locus = inner.locus;
			}
			else if (CdbPathLocus_IsSegmentGeneral(inner.locus))
			{
				CdbPathLocus_MakeSingleQE(&inner.locus,
										  CdbPathLocus_CommonSegments(outer.locus,
																	  inner.locus));
				inner.path->locus = inner.locus;
			}
			else
				goto fail;
			outer.ok_to_replicate = false;
			inner.path = add_rowid_to_path(root, inner.path, p_rowidexpr_id);
			*p_inner_path = inner.path;
			break;

		default:

			/*
			 * The caller should already have transformed
			 * JOIN_UNIQUE_INNER/OUTER into JOIN_INNER
			 */
			elog(ERROR, "unexpected join type %d", jointype);
	}

	/* Get rel sizes. */
	outer.bytes = outer.path->rows * outer.path->pathtarget->width;
	inner.bytes = inner.path->rows * inner.path->pathtarget->width;

	if (join_quals_contain_outer_references ||
		CdbPathLocus_IsOuterQuery(outer.locus) ||
		CdbPathLocus_IsOuterQuery(inner.locus))
	{
		/*
		 * CBDB_FIXME: Consider Replicated locus.
		 * Replicated join OuterQuery, make Replicated to OuterQuery locus may be wrong.
		 * OuterQuery will be finally be Broadcast or Gathered.
		 * If it's Gathered, we will insert/update/delete only on one segment for a replicated table, that's not right.
		 * Ex: insert into a replicated table join with OuterQuery subslect.
		 */
		if (CdbPathLocus_IsReplicated(outer.locus) || CdbPathLocus_IsReplicated(inner.locus))
			goto fail;
	}

	if (join_quals_contain_outer_references)
	{
		if (CdbPathLocus_IsOuterQuery(outer.locus) &&
			CdbPathLocus_IsOuterQuery(inner.locus))
			return outer.locus;

		if (!CdbPathLocus_IsOuterQuery(outer.locus))
			CdbPathLocus_MakeOuterQuery(&outer.move_to);
		if (!CdbPathLocus_IsOuterQuery(inner.locus))
			CdbPathLocus_MakeOuterQuery(&inner.move_to);
	}
	else if (CdbPathLocus_IsOuterQuery(outer.locus) ||
			 CdbPathLocus_IsOuterQuery(inner.locus))
	{
		/*
		 * If one side of the join has "outer query" locus, must bring the
		 * other side there too.
		 */
		if (CdbPathLocus_IsOuterQuery(outer.locus) &&
			CdbPathLocus_IsOuterQuery(inner.locus))
			return outer.locus;

		if (CdbPathLocus_IsOuterQuery(outer.locus))
			inner.move_to = outer.locus;
		else
			outer.move_to = inner.locus;
	}
	else if (CdbPathLocus_IsReplicated(outer.locus) ||
			 CdbPathLocus_IsReplicated(inner.locus))
	{
		/*
		 * Replicated locus could happen here before we add Motion for join.
		 * Ex: insert/update/delete a replicated table with returning and join with others.
		 * We must broadcast to all segments for replicated table, so the upper node have
		 * the Replicated locus.
		 */

		/*
		 * CBDB only allow to modify one CTE now limited by gramma, but in case that there
		 * are multiple references for writeable CTE. We couldn't handle that now.
		 */
		if ((CdbPathLocus_IsReplicated(outer.locus) && CdbPathLocus_IsReplicated(inner.locus)))
			goto fail;

		CdbpathMfjRel *replicated = &outer;
		CdbpathMfjRel *other = &inner;
		if (CdbPathLocus_IsReplicated(inner.locus))
		{
			replicated = &inner;
			other = &outer;
		}

		if (CdbPathLocus_IsSegmentGeneral(other->locus))
		{
			/*
			 * If it's not ok to replicate(outer join) or the numsegments of SegmentGeneral is less than Replicated, gather them to SingleQE.
			 * Don't worry about operation on all segments for replicated table, there will be a Explicit Gather Motion to guarantee that.
			 */
			if(!replicated->ok_to_replicate ||
				!other->ok_to_replicate ||
				(CdbPathLocus_NumSegments(other->locus) < CdbPathLocus_NumSegments(replicated->locus))) 
			{
				CdbPathLocus_MakeSingleQE(&replicated->move_to, CdbPathLocus_NumSegments(replicated->locus));
				CdbPathLocus_MakeSingleQE(&other->move_to, CdbPathLocus_NumSegments(other->locus));
			}
			else 
				return cdbpathlocus_join(jointype, replicated->locus, other->locus);
		}
		else if (CdbPathLocus_IsGeneral(other->locus))
		{
			/*
			 * Quite similar to SegementGeneral and we don't need to care about num segments.
			 * And we must Gather segment to that as SingleQE to Entry Motion may be elided, see changes in cdbpathlocus_join.
			 */
			if(!replicated->ok_to_replicate || !other->ok_to_replicate)
			{
				CdbPathLocus_MakeSingleQE(&replicated->move_to, CdbPathLocus_NumSegments(replicated->locus));
				CdbPathLocus_MakeSingleQE(&other->move_to, CdbPathLocus_NumSegments(replicated->locus));
			}
			else
				return cdbpathlocus_join(jointype, replicated->locus, other->locus);
		}
		else if (CdbPathLocus_IsSingleQE(other->locus) || CdbPathLocus_IsEntry(other->locus))
		{
			/*
			 * Bring to SingleQE and we should guarantee not to be elided to Entry early.
			 * Let cdbpathlocus_join() do it after Motion added.
			 */
			CdbPathLocus_MakeSingleQE(&replicated->move_to, CdbPathLocus_NumSegments(replicated->locus));
		}
		else if (CdbPathLocus_IsPartitioned(other->locus))
		{
			/*
			 * Hashed, Strewn, HashedOJ are similar. 
			 * Redistribute Partition to the num segments of Replicated if num segments are not matched.
			 */
			if (!replicated->ok_to_replicate)
			{
				CdbPathLocus_MakeSingleQE(&replicated->move_to, CdbPathLocus_NumSegments(replicated->locus));
				CdbPathLocus_MakeSingleQE(&other->move_to, CdbPathLocus_NumSegments(other->locus));
			}
			else if (CdbPathLocus_NumSegments(other->locus) != CdbPathLocus_NumSegments(replicated->locus)) 
			{
				CdbPathLocus_MakeHashed(&other->move_to, other->locus.distkey,
									CdbPathLocus_NumSegments(replicated->locus), 0);
			}
			else
			{
				/* Compatible! */
				return other->locus;
			}
		}
		else
		{
			Assert(false);
			/* Shouldn't get here */
			goto fail;
		}
	}
	else if (CdbPathLocus_IsGeneral(outer.locus) ||
			 CdbPathLocus_IsGeneral(inner.locus))
	{
		/*
		 * Motion not needed if either source is everywhere (e.g. a constant).
		 *
		 * But if a row is everywhere and is preserved in an outer join, we don't
		 * want to preserve it in every qExec process where it is unmatched,
		 * because that would produce duplicate null-augmented rows. So in that
		 * case, bring all the partitions to a single qExec to be joined. CDB
		 * TODO: Can this case be handled without introducing a bottleneck?
		 */
		/*
		 * The logic for the join result's locus is (outer's locus is general):
		 *   1. if outer is ok to replicated, then result's locus is the same
		 *      as inner's locus
		 *   2. if outer is not ok to replicated (like left join or wts cases)
		 *      2.1 if inner's locus is hashed or hashOJ, we try to redistribute
		 *          outer as the inner, if fails, make inner singleQE
		 *      2.2 if inner's locus is strewn, we try to redistribute
		 *          outer and inner, if fails, make inner singleQE
		 *      2.3 just return the inner's locus, no motion is needed
		 */
		CdbpathMfjRel *general = &outer;
		CdbpathMfjRel *other = &inner;

		/*
		 * both are general, the result is general
		 */
		if (CdbPathLocus_IsGeneral(outer.locus) &&
			CdbPathLocus_IsGeneral(inner.locus))
			return outer.locus;

		if (CdbPathLocus_IsGeneral(inner.locus))
		{
			general = &inner;
			other = &outer;
		}

		/* numsegments of General locus is always -1 */
		Assert(CdbPathLocus_NumSegments(general->locus) == -1);

		/*
		 * If general can happen everywhere (ok_to_replicate)
		 * then it acts like identity: 
		 *     General join other_locus => other_locus
		 */
		if (general->ok_to_replicate)
			return other->locus;

		if (!CdbPathLocus_IsPartitioned(other->locus))
		{
			/*
			 * If general is not ok_to_replicate, for example,
			 * generate_series(1, 10) left join xxxx, only for
			 * some specific locus types general can act as
			 * identity:
			 *    General join other_locus => other_locus, if and only if
			 *    other_locus in (singleQE, Entry).
			 * Here other's locus:
			 *    - cannot be general (it has already handled)
			 *    - cannot be replicated (assert at the beginning of the function)
			 */
			Assert(CdbPathLocus_IsBottleneck(other->locus) ||
				   CdbPathLocus_IsSegmentGeneral(other->locus));
			return other->locus;
		}
		/*
		 * If other's locus is partitioned, we first try to
		 * add redistribute motion, if fails, we gather other
		 * to singleQE.
		 */
		else if (!try_redistribute(root, general, other, redistribution_clauses, false))
		{
			/*
			 * FIXME: do we need test other's movable?
			 */
			CdbPathLocus_MakeSingleQE(&other->move_to,
									  CdbPathLocus_NumSegments(other->locus));
		}
	}
	else if (CdbPathLocus_IsSegmentGeneral(outer.locus) ||
			 CdbPathLocus_IsSegmentGeneral(inner.locus))
	{
		/*
		 * the whole branch handles the case that at least
		 * one of the two locus is SegmentGeneral. The logic
		 * is:
		 *   - if both are SegmentGeneral:
		 *       1. if both locus are equal, no motion needed, simply return
		 *       2. For update cases. If resultrelation
		 *          is SegmentGeneral, the update must execute
		 *          on each segment of the resultrelation, if resultrelation's
		 *          numsegments is larger, the only solution is to broadcast
		 *          other
		 *       3. no motion is needed, change both numsegments to common
		 *   - if only one of them is SegmentGeneral :
		 *       1. consider update case, if resultrelation is SegmentGeneral,
		 *          the only solution is to broadcast the other
		 *       2. if other's locus is singleQE or entry, make SegmentGeneral
		 *          to other's locus
		 *       3. the remaining possibility of other's locus is partitioned
		 *          3.1 if SegmentGeneral is not ok_to_replicate, try to
		 *              add redistribute motion, if fails gather each to
		 *              singleQE
		 *          3.2 if SegmentGeneral's numsegments is larger, just return
		 *              other's locus
		 *          3.3 try to add redistribute motion, if fails, gather each
		 *              to singleQE
		 */
		CdbpathMfjRel *segGeneral;
		CdbpathMfjRel *other;

		if (CdbPathLocus_IsSegmentGeneral(outer.locus) &&
			CdbPathLocus_IsSegmentGeneral(inner.locus))
		{
			/*
			 * use_common to indicate whether we should
			 * return a segmentgeneral locus with common
			 * numsegments.
			 */
			bool use_common = true;
			/*
			 * Handle the case two same locus
			 */
			if (CdbPathLocus_NumSegments(outer.locus) ==
				CdbPathLocus_NumSegments(inner.locus))
				return inner.locus;
			/*
			 * Now, two locus' numsegments not equal
			 * We should consider update resultrelation
			 * if update,
			 *   - resultrelation's numsegments larger, then
			 *     we should broadcast the other
			 *   - otherwise, results is common
			 * else:
			 *   common
			 */
			if (root->upd_del_replicated_table > 0)
			{
				if ((CdbPathLocus_NumSegments(outer.locus) >
					 CdbPathLocus_NumSegments(inner.locus)) &&
					bms_is_member(root->upd_del_replicated_table,
								  outer.path->parent->relids))
				{
					/*
					 * the updated resultrelation is replicated table
					 * and its numsegments is larger, we should broadcast
					 * the other path
					 */
					if (!inner.ok_to_replicate)
						goto fail;

					/*
					 * FIXME: do we need to test inner's movable?
					 */
					CdbPathLocus_MakeReplicated(&inner.move_to,
												CdbPathLocus_NumSegments(outer.locus), 0);
					use_common = false;
				}
				else if ((CdbPathLocus_NumSegments(outer.locus) <
						  CdbPathLocus_NumSegments(inner.locus)) &&
						 bms_is_member(root->upd_del_replicated_table,
									   inner.path->parent->relids))
				{
					/*
					 * the updated resultrelation is replicated table
					 * and its numsegments is larger, we should broadcast
					 * the other path
					 */
					if (!outer.ok_to_replicate)
						goto fail;

					/*
					 * FIXME: do we need to test outer's movable?
					 */
					CdbPathLocus_MakeReplicated(&outer.move_to,
												CdbPathLocus_NumSegments(inner.locus), 0);
					use_common = false;
				}
			}

			if (use_common)
			{
				/*
				 * The statement is not update a replicated table.
				 * Just return the segmentgeneral with a smaller numsegments.
				 */
				numsegments = CdbPathLocus_CommonSegments(inner.locus,
														  outer.locus);
				outer.locus.numsegments = numsegments;
				inner.locus.numsegments = numsegments;

				return inner.locus;
			}
		}
		else
		{
			if (CdbPathLocus_IsSegmentGeneral(outer.locus))
			{
				segGeneral = &outer;
				other = &inner;
			}
			else
			{
				segGeneral = &inner;
				other = &outer;
			}

			Assert(CdbPathLocus_IsBottleneck(other->locus) ||
				   CdbPathLocus_IsPartitioned(other->locus));

			/*
			 * For UPDATE/DELETE, replicated table can't guarantee a logic row has
			 * same ctid or item pointer on each copy. If we broadcast matched tuples
			 * to all segments, the segments may update the wrong tuples or can't
			 * find a valid tuple according to ctid or item pointer.
			 *
			 * So For UPDATE/DELETE on replicated table, we broadcast other path so
			 * all target tuples can be selected on all copys and then be updated
			 * locally.
			 */
			if (root->upd_del_replicated_table > 0 &&
				bms_is_member(root->upd_del_replicated_table,
							  segGeneral->path->parent->relids))
			{
				/*
				 * For UPDATE on a replicated table, we have to do it
				 * everywhere so that for each segment, we have to collect
				 * all the information of other that is we should broadcast it
				 */

				/*
				 * FIXME: do we need to test other's movable?
				 */
				CdbPathLocus_MakeReplicated(&other->move_to,
											CdbPathLocus_NumSegments(segGeneral->locus), 0);
			}
			else if (CdbPathLocus_IsBottleneck(other->locus))
			{
				/*
				 * if the locus type is equal and segment count is unequal,
				 * we will dispatch the one on more segments to the other
				 */
				numsegments = CdbPathLocus_CommonSegments(segGeneral->locus,
														  other->locus);
				segGeneral->move_to = other->locus;
				segGeneral->move_to.numsegments = numsegments;
			}
			else
			{
				/*
				 * This branch handles for partitioned other locus
				 * hashed, hashoj, strewn
				 */
				Assert(CdbPathLocus_IsPartitioned(other->locus));

				if (!segGeneral->ok_to_replicate)
				{
					if (!try_redistribute(root, segGeneral,
										  other, redistribution_clauses, false))
					{
						/*
						 * FIXME: do we need to test movable?
						 */
						CdbPathLocus_MakeSingleQE(&segGeneral->move_to,
												  CdbPathLocus_NumSegments(segGeneral->locus));
						CdbPathLocus_MakeSingleQE(&other->move_to,
												  CdbPathLocus_NumSegments(other->locus));
					}
				}
				else
				{
					/*
					 * If all other's segments have segGeneral stored, then no motion
					 * is needed.
					 *
					 * A sql to reach here:
					 *     select * from d2 a join r1 b using (c1);
					 * where d2 is a replicated table on 2 segment,
					 *       r1 is a random table on 1 segments.
					 */
					if (CdbPathLocus_NumSegments(segGeneral->locus) >=
						CdbPathLocus_NumSegments(other->locus))
						return other->locus;
					else
					{
						if (!try_redistribute(root, segGeneral,
											  other, redistribution_clauses, false))
						{
							numsegments = CdbPathLocus_CommonSegments(segGeneral->locus,
																	  other->locus);
							/*
							 * FIXME: do we need to test movable?
							 */
							CdbPathLocus_MakeSingleQE(&segGeneral->move_to, numsegments);
							CdbPathLocus_MakeSingleQE(&other->move_to, numsegments);
						}
					}
				}
			}
		}
	}
	/*
	 * Is either source confined to a single process? NB: Motion to a single
	 * process (qDisp or qExec) is the only motion in which we may use Merge
	 * Receive to preserve an existing ordering.
	 */
	else if (CdbPathLocus_IsBottleneck(outer.locus) ||
			 CdbPathLocus_IsBottleneck(inner.locus))
	{ /* singleQE or entry db */
		CdbpathMfjRel *single = &outer;
		CdbpathMfjRel *other = &inner;
		bool single_immovable = (outer.require_existing_order &&
								 !outer_pathkeys) ||
								outer.has_wts;
		bool other_immovable = inner.require_existing_order &&
							   !inner_pathkeys;

		/*
		 * If each of the sources has a single-process locus, then assign both
		 * sources and the join to run in the same process, without motion.
		 * The slice will be run on the entry db if either source requires it.
		 */
		if (CdbPathLocus_IsEntry(single->locus))
		{
			if (CdbPathLocus_IsBottleneck(other->locus))
				return single->locus;
		}
		else if (CdbPathLocus_IsSingleQE(single->locus))
		{
			if (CdbPathLocus_IsBottleneck(other->locus))
			{
				/*
				 * Can join directly on one of the common segments.
				 */
				numsegments = CdbPathLocus_CommonSegments(outer.locus,
														  inner.locus);

				other->locus.numsegments = numsegments;
				return other->locus;
			}
		}

		/* Let 'single' be the source whose locus is singleQE or entry. */
		else
		{
			CdbSwap(CdbpathMfjRel *, single, other);
			CdbSwap(bool, single_immovable, other_immovable);
		}

		Assert(CdbPathLocus_IsBottleneck(single->locus));
		Assert(CdbPathLocus_IsPartitioned(other->locus));

		/* If the bottlenecked rel can't be moved, bring the other rel to it. */
		if (single_immovable)
		{
			if (other_immovable)
				goto fail;
			else
				other->move_to = single->locus;
		}

		/* Redistribute single rel if joining on other rel's partitioning key */
		else if (cdbpath_match_preds_to_distkey(root,
												redistribution_clauses,
												other->path,
												other->locus,
												single->locus,
												false,			   /* parallel_aware */
												&single->move_to)) /* OUT */
		{
			AssertEquivalent(CdbPathLocus_NumSegments(other->locus),
							 CdbPathLocus_NumSegments(single->move_to));
		}

		/* Replicate single rel if cheaper than redistributing both rels. */
		else if (single->ok_to_replicate &&
				 (single->bytes * CdbPathLocus_NumSegments(other->locus) <
				  single->bytes + other->bytes))
			CdbPathLocus_MakeReplicated(&single->move_to,
										CdbPathLocus_NumSegments(other->locus), 0);

		/*
		 * Redistribute both rels on equijoin cols.
		 *
		 * Redistribute both to the same segments, here we choose the
		 * same segments with other.
		 */
		else if (!other_immovable &&
				 cdbpath_distkeys_from_preds(root,
											 redistribution_clauses,
											 single->path,
											 CdbPathLocus_NumSegments(other->locus),
											 0,				   /* parallel_workers */
											 false,			   /* parallel_aware */
											 &single->move_to, /* OUT */
											 &other->move_to)) /* OUT */
		{
			/* ok */
		}

		/* Broadcast single rel for below cases. */
		else if (single->ok_to_replicate &&
				 (other_immovable ||
				  single->bytes < other->bytes ||
				  other->has_wts))
			CdbPathLocus_MakeReplicated(&single->move_to,
										CdbPathLocus_NumSegments(other->locus), 0);

		/* Last resort: If possible, move all partitions of other rel to single QE. */
		else if (!other_immovable)
			other->move_to = single->locus;
		else
			goto fail;
	} /* singleQE or entry */

	/*
	 * No motion if partitioned alike and joining on the partitioning keys.
	 */
	else if (cdbpath_match_preds_to_both_distkeys(root, redistribution_clauses,
												  outer.locus, inner.locus, false))
		return cdbpathlocus_join(jointype, outer.locus, inner.locus);

	/*
	 * Both sources are partitioned.  Redistribute or replicate one or both.
	 */
	else
	{ /* partitioned */
		CdbpathMfjRel *large_rel = &outer;
		CdbpathMfjRel *small_rel = &inner;

		/* Which rel is bigger? */
		if (large_rel->bytes < small_rel->bytes)
			CdbSwap(CdbpathMfjRel *, large_rel, small_rel);

		/* Both side are distribued in 1 segment, it can join without motion. */
		if (CdbPathLocus_NumSegments(large_rel->locus) == 1 &&
			CdbPathLocus_NumSegments(small_rel->locus) == 1)
			return large_rel->locus;

		/* If joining on larger rel's partitioning key, redistribute smaller. */
		if (!small_rel->require_existing_order &&
			cdbpath_match_preds_to_distkey(root,
										   redistribution_clauses,
										   large_rel->path,
										   large_rel->locus,
										   small_rel->locus,
										   false,				 /* parallel_aware */
										   &small_rel->move_to)) /* OUT */
		{
			AssertEquivalent(CdbPathLocus_NumSegments(large_rel->locus),
							 CdbPathLocus_NumSegments(small_rel->move_to));
		}

		/*
		 * Replicate smaller rel if cheaper than redistributing larger rel.
		 * But don't replicate a rel that is to be preserved in outer join.
		 */
		else if (!small_rel->require_existing_order &&
				 small_rel->ok_to_replicate &&
				 (small_rel->bytes * CdbPathLocus_NumSegments(large_rel->locus) <
				  large_rel->bytes))
			CdbPathLocus_MakeReplicated(&small_rel->move_to,
										CdbPathLocus_NumSegments(large_rel->locus), 0);

		/*
		 * Replicate larger rel if cheaper than redistributing smaller rel.
		 * But don't replicate a rel that is to be preserved in outer join.
		 */
		else if (!large_rel->require_existing_order &&
				 large_rel->ok_to_replicate &&
				 (large_rel->bytes * CdbPathLocus_NumSegments(small_rel->locus) <
				  small_rel->bytes))
			CdbPathLocus_MakeReplicated(&large_rel->move_to,
										CdbPathLocus_NumSegments(small_rel->locus), 0);

		/* If joining on smaller rel's partitioning key, redistribute larger. */
		else if (!large_rel->require_existing_order &&
				 cdbpath_match_preds_to_distkey(root,
												redistribution_clauses,
												small_rel->path,
												small_rel->locus,
												large_rel->locus,
												false,				  /* parallel_aware */
												&large_rel->move_to)) /* OUT */
		{
			AssertEquivalent(CdbPathLocus_NumSegments(small_rel->locus),
							 CdbPathLocus_NumSegments(large_rel->move_to));
		}

		/* Replicate smaller rel if cheaper than redistributing both rels. */
		else if (!small_rel->require_existing_order &&
				 small_rel->ok_to_replicate &&
				 (small_rel->bytes * CdbPathLocus_NumSegments(large_rel->locus) <
				  small_rel->bytes + large_rel->bytes))
			CdbPathLocus_MakeReplicated(&small_rel->move_to,
										CdbPathLocus_NumSegments(large_rel->locus), 0);

		/* Replicate larger rel if cheaper than redistributing both rels. */
		else if (!large_rel->require_existing_order &&
				 large_rel->ok_to_replicate &&
				 (large_rel->bytes * CdbPathLocus_NumSegments(small_rel->locus) <
				  large_rel->bytes + small_rel->bytes))
			CdbPathLocus_MakeReplicated(&large_rel->move_to,
										CdbPathLocus_NumSegments(small_rel->locus), 0);

		/*
		 * Redistribute both rels on equijoin cols.
		 *
		 * the two results should all be distributed on the same segments,
		 * here we make them the same with common segments for safe
		 * TODO: how about distribute them both to ALL segments?
		 */
		else if (!small_rel->require_existing_order &&
				 !small_rel->has_wts &&
				 !large_rel->require_existing_order &&
				 !large_rel->has_wts &&
				 cdbpath_distkeys_from_preds(root,
											 redistribution_clauses,
											 large_rel->path,
											 CdbPathLocus_CommonSegments(large_rel->locus,
																		 small_rel->locus),
											 0,		/* parallel_workers */
											 false, /* parallel_aware */
											 &large_rel->move_to,
											 &small_rel->move_to))
		{
			/* ok */
		}

		/*
		 * No usable equijoin preds, or couldn't consider the preferred
		 * motion. Replicate one rel if possible. MPP TODO: Consider number of
		 * seg dbs per host.
		 */
		else if (!small_rel->require_existing_order &&
				 small_rel->ok_to_replicate)
			CdbPathLocus_MakeReplicated(&small_rel->move_to,
										CdbPathLocus_NumSegments(large_rel->locus), 0);
		else if (!large_rel->require_existing_order &&
				 large_rel->ok_to_replicate)
			CdbPathLocus_MakeReplicated(&large_rel->move_to,
										CdbPathLocus_NumSegments(small_rel->locus), 0);

		/* Last resort: Move both rels to a single qExec
		 * only if there is no wts on either rels*/
		else if (!outer.has_wts && !inner.has_wts)
		{
			int numsegments = CdbPathLocus_CommonSegments(outer.locus,
														  inner.locus);
			CdbPathLocus_MakeSingleQE(&outer.move_to, numsegments);
			CdbPathLocus_MakeSingleQE(&inner.move_to, numsegments);
		}
		else
			goto fail;
	}							/* partitioned */

	/*
	 * Move outer.
	 */
	if (!CdbPathLocus_IsNull(outer.move_to))
	{
		outer.path = cdbpath_create_motion_path(root,
												outer.path,
												outer_pathkeys,
												outer.require_existing_order,
												outer.move_to);
		if (!outer.path) /* fail if outer motion not feasible */
			goto fail;

		if (IsA(outer.path, MaterialPath) && !root->config->may_rescan)
		{
			/*
			 * If we are the outer path and can never be rescanned,
			 * we could remove the materialize path.
			 */
			MaterialPath *mpath = (MaterialPath *)outer.path;
			outer.path = mpath->subpath;
		}
	}

	/*
	 * Move inner.
	 */
	if (!CdbPathLocus_IsNull(inner.move_to))
	{
		inner.path = cdbpath_create_motion_path(root,
												inner.path,
												inner_pathkeys,
												inner.require_existing_order,
												inner.move_to);
		if (!inner.path) /* fail if inner motion not feasible */
			goto fail;
	}

	/*
	 * Ok to join.  Give modified subpaths to caller.
	 */
	*p_outer_path = outer.path;
	*p_inner_path = inner.path;

	/* Tell caller where the join will be done. */
	return cdbpathlocus_join(jointype, outer.path->locus, inner.path->locus);

fail: /* can't do this join */
	CdbPathLocus_MakeNull(&outer.move_to);
	return outer.move_to;
} /* cdbpath_motion_for_join */

/*
 * Does the path contain WorkTableScan?
 */
bool
cdbpath_contains_wts(Path *path)
{
	JoinPath   *joinPath;
	AppendPath *appendPath;
	ListCell   *lc;

	if (IsJoinPath(path))
	{
		joinPath = (JoinPath *) path;
		if (cdbpath_contains_wts(joinPath->outerjoinpath)
			|| cdbpath_contains_wts(joinPath->innerjoinpath))
			return true;
		else
			return false;
	}
	else if (IsA(path, AppendPath))
	{
		appendPath = (AppendPath *) path;
		foreach(lc, appendPath->subpaths)
		{
			if (cdbpath_contains_wts((Path *) lfirst(lc)))
				return true;
		}
		return false;
	}

	return path->pathtype == T_WorkTableScan;
}


/*
 * has_redistributable_clause
 *	  If the restrictinfo's clause is redistributable, return true.
 */
bool
has_redistributable_clause(RestrictInfo *restrictinfo)
{
	return restrictinfo->hashjoinoperator != InvalidOid;
}

/*
 * try_redistribute
 *     helper function for A join B when
 *     - A's locus is general or segmentgeneral
 *     - B's locus is partitioned
 *     it tries to redistribute A to B's locus
 *     or redistribute both A and B to the same
 *     partitioned locus.
 *
 *     return values:
 *     - true: redistributed motion has been added for A
 *     - false: cannot add redistributed motion, caller should
 *       continue to find other solutions.
 */
static bool
try_redistribute(PlannerInfo *root, CdbpathMfjRel *g, CdbpathMfjRel *o,
				 List *redistribution_clauses, bool parallel_aware)
{
	bool g_immovable;
	bool o_immovable;

	Assert(CdbPathLocus_IsGeneral(g->locus) ||
		   CdbPathLocus_IsSegmentGeneral(g->locus) ||
		   CdbPathLocus_IsSegmentGeneralWorkers(g->locus));
	Assert(CdbPathLocus_IsPartitioned(o->locus));

	/* CBDB_PARALLEL_FIXME: is it possible to redistribute both ?*/
	if (CdbPathLocus_IsHashedWorkers(o->locus))
		return false;

	/*
	 * we cannot add motion if requiring order.
	 * has_wts can be true only for general locus
	 * otherwise, it is false and not impact the
	 * value of <x>_immovable.
	 */
	g_immovable = (g->require_existing_order &&
				   !g->pathkeys) || g->has_wts;

	/*
	 * if g cannot be added motion on,
	 * we should return immediately.
	 */
	if (g_immovable)
		return false;
	
	o_immovable = (o->require_existing_order &&
				   !o->pathkeys) || o->has_wts;

	if (CdbPathLocus_IsHashed(o->locus) ||
		CdbPathLocus_IsHashedOJ(o->locus))
	{
		/*
		 * first try to only redistribute g as o's locus
		 * if fails then try to redistribute both g and o
		 */
		if (cdbpath_match_preds_to_distkey(root,
										   redistribution_clauses,
										   o->path,
										   o->locus,
										   g->locus,
										   parallel_aware,
										   &g->move_to))
			return true;
		else
		{
			/*
			 * both g and o can be added motion on,
			 * we should try each possible case.
			 */
			int			numsegments;

			if (CdbPathLocus_IsGeneral(g->locus))
				numsegments = CdbPathLocus_NumSegments(o->locus);
			else
				numsegments = CdbPathLocus_CommonSegments(o->locus, g->locus);

			if(cdbpath_distkeys_from_preds(root,
										   redistribution_clauses,
										   o->path,
										   numsegments,
										   Max(o->path->parallel_workers, g->path->parallel_workers),
										   parallel_aware,
										   &o->move_to,
										   &g->move_to))
			{
				return true;
			}
		}
	}
	else
	{
		/*
		 * the only possible solution is to
		 * redistributed both g and o, so
		 * both g and o should be movable.
		 */
		int			numsegments;

		if (CdbPathLocus_IsGeneral(g->locus))
			numsegments = CdbPathLocus_NumSegments(o->locus);
		else
			numsegments = CdbPathLocus_CommonSegments(o->locus, g->locus);

		if (!o_immovable &&
			cdbpath_distkeys_from_preds(root,
										redistribution_clauses,
										o->path,
										numsegments,
										Max(o->path->parallel_workers, g->path->parallel_workers),
										parallel_aware,
										&o->move_to,
										&g->move_to))
		{
			return true;
		}
	}

	/*
	 * fail to redistribute, return false
	 * to let caller know.
	 */
	return false;
}

/*
 * Add a suitable Motion Path so that the input tuples from 'subpath' are
 * distributed correctly for insertion into target table.
 */
Path *
create_motion_path_for_ctas(PlannerInfo *root, GpPolicy *policy, Path *subpath)
{
	GpPolicyType	policyType = policy->ptype;

	if (policyType == POLICYTYPE_PARTITIONED && policy->nattrs == 0)
	{
		/*
		 * If the target table is DISTRIBUTED RANDOMLY, and the input data
		 * is already partitioned, we could let the insertions happen where
		 * they are. But to ensure more random distribution, redistribute.
		 * This is different from create_motion_path_for_insert().
		 */
		CdbPathLocus targetLocus;

		CdbPathLocus_MakeStrewn(&targetLocus, policy->numsegments, 0);
		return cdbpath_create_motion_path(root, subpath, NIL, false, targetLocus);
	}
	else
		return create_motion_path_for_insert(root, policy, subpath);
}

/*
 * Add a suitable Motion Path so that the input tuples from 'subpath' are
 * distributed correctly for insertion into target table.
 */
Path *
create_motion_path_for_insert(PlannerInfo *root, GpPolicy *policy,
							  Path *subpath)
{
	GpPolicyType	policyType = policy->ptype;
	CdbPathLocus	targetLocus;

	if (policyType == POLICYTYPE_PARTITIONED)
	{
		/*
		 * A query to reach here: INSERT INTO t1 VALUES(1).
		 * There is no need to add a motion from General, we could
		 * simply put General on the same segments with target table.
		 */
		/* FIXME: also do this for other targetPolicyType? */
		/* FIXME: also do this for all the subplans */
		if (CdbPathLocus_IsGeneral(subpath->locus))
		{
			subpath->locus.numsegments = policy->numsegments;
		}

		targetLocus = cdbpathlocus_for_insert(root, policy, subpath->pathtarget);

		if (policy->nattrs == 0 && CdbPathLocus_IsPartitioned(subpath->locus))
		{
			/*
			 * If the target table is DISTRIBUTED RANDOMLY, we can insert the
			 * rows anywhere. So if the input path is already partitioned, let
			 * the insertions happen where they are. Unless the GUC gp_force_random_redistribution
			 * tells us to force the redistribution.
			 *
			 * If you `explain` the query insert into tab_random select * from tab_partition
			 * there is not Motion node in plan. However, it is not means that the query only
			 * execute in entry db. It is dispatched to QE and do everything well as we expect.
			 *
			 * But, we need to grant a Motion node if target locus' segnumber is different with
			 * subpath.
			 */
			if (gp_force_random_redistribution || targetLocus.numsegments != subpath->locus.numsegments)
			{
				CdbPathLocus_MakeStrewn(&targetLocus, policy->numsegments, 0);
				subpath = cdbpath_create_motion_path(root, subpath, NIL, false, targetLocus);
			}
		}
		else if (CdbPathLocus_IsNull(targetLocus))
		{
			/* could not create DistributionKeys to represent the distribution keys. */
			CdbPathLocus_MakeStrewn(&targetLocus, policy->numsegments, 0);

			subpath = (Path *) make_motion_path(root, subpath, targetLocus, false, policy);
		}
		else
		{
			subpath = cdbpath_create_motion_path(root, subpath, NIL, false, targetLocus);
		}
	}
	else if (policyType == POLICYTYPE_ENTRY)
	{
		/*
		 * Query result needs to be brought back to the QD.
		 */
		CdbPathLocus_MakeEntry(&targetLocus);
		subpath = cdbpath_create_motion_path(root, subpath, NIL, false, targetLocus);
	}
	else if (policyType == POLICYTYPE_REPLICATED)
	{
		/* try to optimize insert with no motion introduced into */
		if (optimizer_replicated_table_insert &&
			!contain_volatile_functions((Node *)subpath->pathtarget->exprs) &&
			!contain_volatile_functions((Node *)root->parse->havingQual))
		{
			/* doesn't support insert parallel yet. */
			Assert(!CdbPathLocus_IsSegmentGeneralWorkers(subpath->locus));

			/*
			 * CdbLocusType_SegmentGeneral is only used by replicated table
			 * right now, so if both input and target are replicated table,
			 * no need to add a motion.
			 *
			 * Also, to expand a replicated table to new segments, gpexpand
			 * force a data reorganization by a query like:
			 * CREATE TABLE tmp_tab AS SELECT * FROM source_table DISTRIBUTED REPLICATED
			 * Obviously, tmp_tab in new segments can't get data if we don't
			 * add a broadcast here.
			 */
			if(CdbPathLocus_IsSegmentGeneral(subpath->locus) &&
					subpath->locus.numsegments >= policy->numsegments)
			{
				/*
				 * A query to reach here:
				 *     INSERT INTO d1 SELECT * FROM d1;
				 * There is no need to add a motion from General, we
				 * could simply put General on the same segments with
				 * target table.
				 *
				 * Otherwise a broadcast motion is needed otherwise d2 will
				 * only have data on segment 0.
				 */
				subpath->locus.numsegments = policy->numsegments;
				return subpath;
			}

			/* plan's data are available on all segment, no motion needed */
			if(CdbPathLocus_IsGeneral(subpath->locus))
			{
				/*
				 * A query to reach here: INSERT INTO d1 VALUES(1).
				 * There is no need to add a motion from General, we
				 * could simply put General on the same segments with
				 * target table.
				 */
				subpath->locus.numsegments = Min(subpath->locus.numsegments,policy->numsegments) ;
				return subpath;
			}

		}

		/*
		 * planner may have add a top Motion eariler.
		 * Create table t1(id int) distributed randomly;
		 * Create table t2 as select random() from t1 distributed replicated;
		 * Avoid Motion if there was already one.
		 */
		if (!CdbPathLocus_IsReplicated(subpath->locus))
			subpath = cdbpath_create_broadcast_motion_path(root, subpath, policy->numsegments);
	}
	else
		elog(ERROR, "unrecognized policy type %u", policyType);

	if (CdbPathLocus_IsStrewn(subpath->locus) && subpath->locus.distkey == NIL &&
		gp_random_insert_segments > 0 &&
		gp_random_insert_segments < CdbPathLocus_NumSegments(subpath->locus))
	{
		/* Select limited random segments for data insertion. */
		subpath->locus.numsegments = gp_random_insert_segments;
	}

	return subpath;
}

/*
 * Add a suitable Motion Path for delete and update. If the UPDATE
 * modifies the distribution key columns, use create_split_update_path()
 * instead.
 */
Path *
create_motion_path_for_upddel(PlannerInfo *root, Index rti, GpPolicy *policy,
							  Path *subpath)
{
	GpPolicyType	policyType = policy->ptype;
	CdbPathLocus	targetLocus;

	if (policyType == POLICYTYPE_PARTITIONED)
	{
		if (can_elide_explicit_motion(root, rti, subpath, policy))
			return subpath;
		else
		{
			CdbPathLocus_MakeStrewn(&targetLocus, policy->numsegments, 0);
			subpath = cdbpath_create_explicit_motion_path(root,
														  subpath,
														  targetLocus);
		}
	}
	else if (policyType == POLICYTYPE_ENTRY)
	{
		/* Master-only table */
		CdbPathLocus_MakeEntry(&targetLocus);
		subpath = cdbpath_create_motion_path(root, subpath, NIL, false, targetLocus);
	}
	else if (policyType == POLICYTYPE_REPLICATED)
	{
		/*
		 * The statement that update or delete on replicated table has to
		 * be dispatched to each segment and executed on each segment. Thus
		 * the targetlist cannot contain volatile functions.
		 */
		if (contain_volatile_functions((Node *) (subpath->pathtarget->exprs)))
			elog(ERROR, "could not devise a plan.");
	}
	else
		elog(ERROR, "unrecognized policy type %u", policyType);

	return subpath;
}

/*
 * In Postgres planner, we add a SplitUpdate node at top so that updating on
 * distribution columns could be handled. The SplitUpdate will split each
 * update into delete + insert.
 *
 * There are several important points should be highlighted:
 *
 * First, in order to split each update operation into two operations,
 * delete + insert, we need several junk columns in the subplan's targetlist,
 * in addition to the row's new values:
 *
 * ctid            the tuple id used for deletion
 *
 * gp_segment_id   the segment that the row originates from. Usually the
 *                 current segment where the SplitUpdate runs, but not
 *                 necessarily, if there are multiple joins involved and the
 *                 planner decided redistribute the data.
 *
 * oid             if result relation has oids, the old OID, so that it can be
 *                 preserved in the new row.
 *
 * We will add one more column to the output, the "action". It's an integer
 * that indicates for each row, whether it represents the DELETE or the INSERT
 * of that row. It is generated by the Split Update node.
 *
 * Second, current GPDB executor don't support statement-level update triggers
 * and will skip row-level update triggers because a split-update is actually
 * consist of a delete and insert. So, if the result relation has update
 * triggers, we should reject and error out because it's not functional.
 *
 *
 * For example, a typical plan would be as following for statement:
 * update foo set id = l.v + 1 from dep l where foo.v = l.id:
 *
 * |-- join ( targetlist: [ l.v + 1, foo.v, foo.ctid, foo.gp_segment_id ] )
 *       |
 *       |-- motion ( targetlist: [l.id, l.v] )
 *       |    |
 *       |    |-- seqscan on dep ....
 *       |
 *       |-- hash (targetlist [ foo.v, foo.ctid, foo.gp_segment_id ] )
 *            |
 *            |-- seqscan on foo (targetlist: [ foo.v, foo.ctid, foo.gp_segment_id ] )
 *
 * From the plan above, the target foo.id is assigned as l.v + 1, and expand_targetlist()
 * ensured that the old value of id, is also available, even though it would not otherwise
 * be needed.
 *
 * 'rti' is the UPDATE target relation.
 */
Path *
create_split_update_path(PlannerInfo *root, Index rti, GpPolicy *policy, Path *subpath)
{
	GpPolicyType	policyType = policy->ptype;
	CdbPathLocus	targetLocus;

	if (policyType == POLICYTYPE_PARTITIONED)
	{
		/*
		 * If any of the distribution key columns are being changed,
		 * the UPDATE might move tuples from one segment to another.
		 * Create a Split Update node to deal with that.
		 *
		 * If the input is a dummy plan that cannot return any rows,
		 * e.g. because the input was eliminated by constraint
		 * exclusion, we can skip it.
		 */
		targetLocus = cdbpathlocus_for_insert(root, policy, subpath->pathtarget);

		subpath = (Path *) make_splitupdate_path(root, subpath, rti);
		subpath = cdbpath_create_explicit_motion_path(root,
													  subpath,
													  targetLocus);
	}
	else if (policyType == POLICYTYPE_ENTRY)
	{
		/* Master-only table */
		CdbPathLocus_MakeEntry(&targetLocus);
		subpath = cdbpath_create_motion_path(root, subpath, NIL, false, targetLocus);
	}
	else if (policyType == POLICYTYPE_REPLICATED)
	{
	}
	else
		elog(ERROR, "unrecognized policy type %u", policyType);
	return subpath;
}

/*
 * turn_volatile_seggen_to_singleqe
 *
 * This function is the key tool to build correct plan
 * for general, segmentgeneral, replicated locus paths that contain
 * volatile functions.
 *
 * If we find such a pattern:
 *    1. if we are update or delete statement on replicated table
 *       simply reject the query
 *    2. if it is general locus, simply change it to singleQE
 *    3. if it is segmentgeneral, use a motion to bring it to
 *       singleQE and then create a projection path
 *
 * If we do not find the pattern, simply return the input path.
 *
 * The last parameter of this function is the part that we want to
 * check volatile functions.
 */
Path *
turn_volatile_seggen_to_singleqe(PlannerInfo *root, Path *path, Node *node)
{
	if ((CdbPathLocus_IsSegmentGeneral(path->locus) ||
		 CdbPathLocus_IsGeneral(path->locus) ||
		 CdbPathLocus_IsReplicated(path->locus) ||
		 CdbPathLocus_IsSegmentGeneralWorkers(path->locus)) &&
		 (contain_volatile_functions(node) || IsA(path, LimitPath)))
	{
		CdbPathLocus     singleQE;
		Path            *mpath;
		ProjectionPath  *ppath;

		if (root->upd_del_replicated_table > 0 &&
			bms_is_member(root->upd_del_replicated_table,
						  path->parent->relids))
			elog(ERROR, "could not devise a plan");

		if (CdbPathLocus_IsGeneral(path->locus))
		{
			CdbPathLocus_MakeSingleQE(&(path->locus),
									  getgpsegmentCount());
			return path;
		}

		CdbPathLocus_MakeSingleQE(&singleQE,
								  CdbPathLocus_NumSegments(path->locus));
		mpath = cdbpath_create_motion_path(root, path, NIL, false, singleQE);
		/*
		 * mpath might be NULL, like path contain outer Params
		 * See Github Issue 13532 for details.
		 */
		if (mpath == NULL)
			return path;
		ppath =  create_projection_path_with_quals(root, mpath->parent, mpath,
												   mpath->pathtarget, NIL, false);
		ppath->force = true;
		return (Path *) ppath;
	}
	else
		return path;
}

static SplitUpdatePath *
make_splitupdate_path(PlannerInfo *root, Path *subpath, Index rti)
{
	RangeTblEntry  *rte;
	PathTarget		*splitUpdatePathTarget;
	SplitUpdatePath	*splitupdatepath;
	DMLActionExpr	*actionExpr;

	/* Suppose we already hold locks before caller */
	rte = planner_rt_fetch(rti, root);

	/*
	 * Firstly, Trigger is not supported officially by Cloudberry.
	 *
	 * Secondly, the update trigger is processed in ExecUpdate.
	 * however, splitupdate will execute ExecSplitUpdate_Insert
	 * or ExecDelete instead of ExecUpdate. So the update trigger
	 * will not be triggered in a split plan.
	 *
	 * PostgreSQL fires the row-level DELETE, INSERT, and BEFORE
	 * UPDATE triggers, but not row-level AFTER UPDATE triggers,
	 * if you UPDATE a partitioning key column.
	 * Doing a similar thing doesn't help Cloudberry likely, the
	 * behavior would be uncertain since some triggers happen on
	 * segments and they may require cross segments data changes.
	 *
	 * So an update trigger is not allowed when updating the
	 * distribution key.
	 */
	if (has_update_triggers(rte->relid, false))
		ereport(ERROR,
				(errcode(ERRCODE_GP_FEATURE_NOT_YET),
				 errmsg("UPDATE on distributed key column not allowed on relation with update triggers")));

	/* Add action column at the end of targetlist */
	actionExpr = makeNode(DMLActionExpr);
	splitUpdatePathTarget = copy_pathtarget(subpath->pathtarget);
	add_column_to_pathtarget(splitUpdatePathTarget, (Expr *) actionExpr, 0);

	/* populate information generated above into splitupdate node */
	splitupdatepath = makeNode(SplitUpdatePath);
	splitupdatepath->path.pathtype = T_SplitUpdate;
	splitupdatepath->path.parent = subpath->parent;
	splitupdatepath->path.pathtarget = splitUpdatePathTarget;
	splitupdatepath->path.param_info = NULL;
	splitupdatepath->path.parallel_aware = false;
	splitupdatepath->path.parallel_safe = subpath->parallel_safe;
	splitupdatepath->path.parallel_workers = subpath->parallel_workers;
	splitupdatepath->path.rows = 2 * subpath->rows;
	splitupdatepath->path.startup_cost = subpath->startup_cost;
	splitupdatepath->path.total_cost = subpath->total_cost;
	splitupdatepath->path.pathkeys = subpath->pathkeys;
	splitupdatepath->path.locus = subpath->locus;
	splitupdatepath->subpath = subpath;
	splitupdatepath->resultRelation = rti;

	return splitupdatepath;
}

static bool
can_elide_explicit_motion(PlannerInfo *root, Index rti, Path *subpath,
						  GpPolicy *policy)
{
	/*
	 * If there are no Motions between scan of the target relation and here,
	 * no motion is required.
	 */
	if (bms_is_member(rti, subpath->sameslice_relids))
		return true;

	if (!CdbPathLocus_IsStrewn(subpath->locus))
	{
		CdbPathLocus    resultrelation_locus = cdbpathlocus_from_policy(root, rti, policy, 0);
		return cdbpathlocus_equal(subpath->locus, resultrelation_locus);
	}

	return false;
}

/*
 * cdbpath_motion_for_parallel_join
 * Sibling of cdbpath_motion_for_join in parallel mode.
 * Separate with non-parallel functions as the logic of parallel join is quite different:
 *  1. Treating path locus by outer/inner. The position side in prallel join is sensitive.
 *  2. Still try Redistribute Motion even if Broadcast one side using parallel_hash_enable_motion_broadcast.
 *  3. Never duplicate outer_path(parallel_workers=0). That will lead to wrong results, ex: parallel left join.
 *     Follow upstream until we have a clear answer.
 *
 * The locus of path whose workers > 1 could be:
 *  HashedWorkers: parallel scan on a Hashed locus table and etc.
 *  ReplicatedWorkers: like Broadcast, replica data to segments but strewn on workers of the same segment.
 *  SegmentGeneralWorkers: parallel scan on a replica table and etc.
 *  Strewn(parallel_workers > 1), parallel scan on a randomly distributed table and etc.
 *  Hashed(parallel_workers > 1), generated by HashedWorkers with a Redistribute Motion.
 *
 * When we add a new xxxWorkers locus?
 * 	ISTM: xxxWorkers means strewn on workers of the same segment, but together as a xxx locus on segments that
 *  could be used to join with other locus as non-parallel plan.
 *  ex: ReplicatedWorkers, all data are replicated on segments, but strewn on workers of a segment.
 *  For Hashed(parallel_workers > 1), it's a little different because data is firstly hashed on segments,
 *  and hashed on parallel_workers of a segment, so the Hashed(parallel_workers) could join with the same
 *  locus without any motions. And it's not strewn on workers.
 *  Another special locus is: Strewn(parallel_workers > 1). Shall we add a StrewnWorkers too?
 *  Since it's already strewn on segments, no matter with more processes.
 *  Another reason is adding a new locus is complex and expensive, we have to handle all the possible locus
 *  joined with that.
 *
 * parallel_aware means parallel hashjoin with a shared hash table.
 *
 * Incompatible locus could be compatible when parallel_aware, ex:
 *               JOIN
 *             /      \
 *  HashedWorkers 	ParallelHash
 *                     \
 *                ReplicatedWorkers
 *  Both sides are strewn on workers of the same segments, but ParallelHash collect all data from workers' processes.
 *  So, outer side could find every matched data. And in this example, the join locus is HashedWorkers.
 *
 * We don't reset path's parallel_workers now.
 *  There was once an idea reseting path's parallel_works to avoid
 * 	Motion if inner and outer's parallel_workers doesn't match.
 * 	But there are a lot of issues we don't have a clear answer.
 *  See https://code.hashdata.xyz/cloudberry/cbdb-postgres-merge/-/issues/43.
 *
 * We couldn't expect the parallel_workers of outer or inner path.
 * Partial path may generate locus(parallel_workers=0) if needed, ex:
 * GP's parallel two stage Group Gather Agg path which will generate a
 * SingleQE locus in the middle plan. And that path could participate in
 * parallel plan with Motion(1:6), but it still can't be processed by multiple
 * workers or be duplicated in every worker as the inner path.
 *
 * All locus test cases are in cbdb_parallel, see final join locus examples there.
 */
CdbPathLocus
cdbpath_motion_for_parallel_join(PlannerInfo *root,
						JoinType jointype,	/* JOIN_INNER/FULL/LEFT/RIGHT/IN */
						Path **p_outer_path,	/* INOUT */
						Path **p_inner_path,	/* INOUT */
						int *p_rowidexpr_id,	/* OUT */
						List *redistribution_clauses, /* equijoin RestrictInfo list */
						List *restrict_clauses,
						List *outer_pathkeys,
						List *inner_pathkeys,
						bool outer_require_existing_order,
						bool inner_require_existing_order,
						bool parallel_aware)
{
	CdbpathMfjRel outer;
	CdbpathMfjRel inner;
	bool		join_quals_contain_outer_references;
	ListCell   *lc;

	*p_rowidexpr_id = 0;

	outer.pathkeys = outer_pathkeys;
	inner.pathkeys = inner_pathkeys;
	outer.path = *p_outer_path;
	inner.path = *p_inner_path;
	outer.locus = outer.path->locus;
	inner.locus = inner.path->locus;
	CdbPathLocus_MakeNull(&outer.move_to);
	CdbPathLocus_MakeNull(&inner.move_to);
	outer.isouter = true;
	inner.isouter = false;
	int outerParallel = outer.locus.parallel_workers;
	int innerParallel = inner.locus.parallel_workers;

	Assert(cdbpathlocus_is_valid(outer.locus));
	Assert(cdbpathlocus_is_valid(inner.locus));
	/*  CBDB_PARALLEL_FIXME: reconsider the meaning of parallel_safe in GP parallel? */
	if (!outer.path->parallel_safe || !inner.path->parallel_safe)
		goto fail;

	/*
	 * Does the join quals contain references to outer query? If so, we must
	 * evaluate them in the outer query's locus. That means pulling both
	 * inputs to outer locus, and performing the join there.
	 *
	 * XXX: If there are pseudoconstant quals, they will be executed by a
	 * gating Result with a One-Time Filter. In that case, the join's inputs
	 * wouldn't need to be brought to the outer locus. We could execute the
	 * join normally, and bring the result to the outer locus and put the
	 * gating Result above the Motion, instead. But for now, we're not smart
	 * like that.
	 */
	join_quals_contain_outer_references = false;
	foreach(lc, restrict_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		if (rinfo->contain_outer_query_references)
		{
			join_quals_contain_outer_references = true;
			break;
		}
	}

	/*
	 * Locus type Replicated/ReplicatedWorkers can only be generated
	 * by join operation.
	 * And in the function cdbpathlocus_join there is a rule:
	 * <any locus type> join <Replicated> => any locus type
	 * Proof by contradiction, it shows that when code arrives here,
	 * it is impossible that any of the two input paths' locus
	 * is Replicated. So we add asserts here.
	 */
	Assert(!CdbPathLocus_IsReplicated(outer.locus));
	Assert(!CdbPathLocus_IsReplicated(inner.locus));
	Assert(!CdbPathLocus_IsReplicatedWorkers(outer.locus));
	Assert(!CdbPathLocus_IsReplicatedWorkers(inner.locus));

	if (CdbPathLocus_IsReplicated(outer.locus) ||
		CdbPathLocus_IsReplicated(inner.locus) ||
		CdbPathLocus_IsReplicatedWorkers(outer.locus) ||
		CdbPathLocus_IsReplicatedWorkers(inner.locus))
		goto fail;

	outer.has_wts = cdbpath_contains_wts(outer.path);
	inner.has_wts = cdbpath_contains_wts(inner.path);

	/* For now, inner path should not contain WorkTableScan */
	Assert(!inner.has_wts);

	/*
	 * If outer rel contains WorkTableScan and inner rel is hash distributed,
	 * unfortunately we have to pretend that inner is randomly distributed,
	 * otherwise we may end up with redistributing outer rel.
	 */
	/* CBDB_PARALLEL_FIXME: this may cause parallel CTE, not sure if it's right */
	if (outer.has_wts && inner.locus.distkey != NIL)
		CdbPathLocus_MakeStrewn(&inner.locus,
								CdbPathLocus_NumSegments(inner.locus),
								inner.path->parallel_workers);

	/*
	 * Caller can specify an ordering for each source path that is the same as
	 * or weaker than the path's existing ordering. Caller may insist that we
	 * do not add motion that would lose the specified ordering property;
	 * otherwise the given ordering is preferred but not required. A required
	 * NIL ordering means no motion is allowed for that path.
	 */
	outer.require_existing_order = outer_require_existing_order;
	inner.require_existing_order = inner_require_existing_order;

	/*
	 * Don't consider replicating the preserved rel of an outer join, or the
	 * current-query rel of a join between current query and subquery.
	 *
	 * Path that contains WorkTableScan cannot be replicated.
	 */
	/* ok_to_replicate means broadcast */
	outer.ok_to_replicate = !outer.has_wts;
	inner.ok_to_replicate = true;

	switch (jointype)
	{
		case JOIN_INNER:
			break;
		case JOIN_SEMI:
			if (!enable_parallel_semi_join)
				goto fail;
			/* FALLTHROUGH */
		case JOIN_ANTI:
		case JOIN_LEFT:
		case JOIN_LASJ_NOTIN:
			outer.ok_to_replicate = false;
			break;
		case JOIN_UNIQUE_OUTER:
		case JOIN_UNIQUE_INNER:
		case JOIN_RIGHT:
		case JOIN_FULL:
			/* Join types are not supported in parallel yet. */
			goto fail;
		case JOIN_DEDUP_SEMI:
			if (!enable_parallel_dedup_semi_join)
				goto fail;

			if (!CdbPathLocus_IsPartitioned(inner.locus))
				goto fail;

			if (CdbPathLocus_IsPartitioned(outer.locus) ||
				CdbPathLocus_IsBottleneck(outer.locus))
			{
				/* ok */
			}
			else if (CdbPathLocus_IsGeneral(outer.locus))
			{
				CdbPathLocus_MakeSingleQE(&outer.locus,
										  CdbPathLocus_NumSegments(inner.locus));
				outer.path->locus = outer.locus;
			}
			else if (CdbPathLocus_IsSegmentGeneral(outer.locus))
			{
				CdbPathLocus_MakeSingleQE(&outer.locus,
										  CdbPathLocus_CommonSegments(inner.locus,
																	  outer.locus));
				outer.path->locus = outer.locus;
			}
			else if (CdbPathLocus_IsSegmentGeneralWorkers(outer.locus))
			{
				/* CBDB_PARALLEL_FIXME: Consider gather from SegmentGeneralWorkers. */
				goto fail;
			}
			else
				goto fail;
			inner.ok_to_replicate = false;

			/*
			 * CBDB_PARALLEL:
			 * rowidexpr is executed by 48 bits of row counter of a 64 bit int.
			 * When in parallel mode, we need to compute the total bits of the
			 * left 16 bits for segments and parallel workers.
			 * The formula is:
			 *  parallel_bits + seg_bits
			 * while segs is max(dbid) across cluster in case that dbid segments
			 * are uncontinuous.
			 * And keep some room to make sure there should not be
			 * duplicated rows when execution.
			 */
			if (outerParallel > 1)
			{
				int segs = cdbcomponent_get_maxdbid();
				int parallel_bits = pg_leftmost_one_pos32(outerParallel) + 1;
				int seg_bits = pg_leftmost_one_pos32(segs) + 1;
				if (parallel_bits + seg_bits > 16)
					goto fail;
			}
			outer.path = add_rowid_to_path(root, outer.path, p_rowidexpr_id);
			*p_outer_path = outer.path;
			break;

		case JOIN_DEDUP_SEMI_REVERSE:
			if (!enable_parallel_dedup_semi_reverse_join)
				goto fail;
			/* same as JOIN_DEDUP_SEMI, but with inner and outer reversed */
			if (!CdbPathLocus_IsPartitioned(outer.locus))
				goto fail;
			if (CdbPathLocus_IsPartitioned(inner.locus) ||
				CdbPathLocus_IsBottleneck(inner.locus))
			{
				/* ok */
			}
			else if (CdbPathLocus_IsGeneral(inner.locus))
			{
				CdbPathLocus_MakeSingleQE(&inner.locus,
										  CdbPathLocus_NumSegments(outer.locus));
				inner.path->locus = inner.locus;
			}
			else if (CdbPathLocus_IsSegmentGeneral(inner.locus))
			{
				CdbPathLocus_MakeSingleQE(&inner.locus,
										  CdbPathLocus_CommonSegments(outer.locus,
																	  inner.locus));
				inner.path->locus = inner.locus;
			}
			else if (CdbPathLocus_IsSegmentGeneralWorkers(inner.locus))
			{
				/* CBDB_PARALLEL_FIXME: Consider gather from SegmentGeneralWorkers. */
				goto fail;
			}
			else
				goto fail;
			outer.ok_to_replicate = false;
			if (innerParallel > 1)
			{
				int segs = cdbcomponent_get_maxdbid();
				int parallel_bits = pg_leftmost_one_pos32(innerParallel) + 1;
				int seg_bits = pg_leftmost_one_pos32(segs) + 1;
				if (parallel_bits + seg_bits > 16)
					goto fail;
			}
			inner.path = add_rowid_to_path(root, inner.path, p_rowidexpr_id);
			*p_inner_path = inner.path;
			break;

		default:
			elog(ERROR, "unexpected join type %d", jointype);
	}

	/* Get rel sizes. */
	outer.bytes = outer.path->rows * outer.path->pathtarget->width;
	inner.bytes = inner.path->rows * inner.path->pathtarget->width;

	if (join_quals_contain_outer_references ||
		CdbPathLocus_IsOuterQuery(outer.locus) ||
		CdbPathLocus_IsOuterQuery(inner.locus) ||
		CdbPathLocus_IsEntry(outer.locus) ||
		CdbPathLocus_IsEntry(inner.locus) ||
		CdbPathLocus_IsGeneral(outer.locus) ||
		CdbPathLocus_IsGeneral(inner.locus))
	{
		/*
		 * Not supported to participate in parallel yet.
		 */
		goto fail;
	}
	/* SegmentGeneralWorkers join others */
	else if (CdbPathLocus_IsSegmentGeneralWorkers(outer.locus))
	{
		CdbpathMfjRel *segGeneral = &outer;
		CdbpathMfjRel *other = &inner;

		Assert(outerParallel > 1);

		if (CdbPathLocus_IsSegmentGeneralWorkers(inner.locus))
		{
			Assert(innerParallel > 1);
			/* We don't handle parallel when expanding segments */
			if (CdbPathLocus_NumSegments(outer.locus) != CdbPathLocus_NumSegments(inner.locus))
				goto fail;
			/*
			 * Couldn't join without shared hash table if both are SegmentGeneralWorkers.
			 * We don't expect a motion for that.
			 */
			if (!parallel_aware)
				goto fail;
			if ((outerParallel != innerParallel))
				goto fail;
			/*
			 * SegmentGeneralWorkers parallel join SegmentGeneralWorkers when parallel_aware
			 * generate SegmentGeneralWorerks locus.
			 * see ex 5_P_5_5 in cbdb_parallel.sql
			 */
			if (outer.ok_to_replicate && inner.ok_to_replicate)
				return outer.locus;
			goto fail;
		}

		if (CdbPathLocus_IsSegmentGeneral(inner.locus))
		{
			Assert(innerParallel <= 1);
			if (parallel_aware)
				goto fail;

			if (CdbPathLocus_NumSegments(outer.locus) != CdbPathLocus_NumSegments(inner.locus))
				goto fail;
			/*
			 * SegmentGeneralWorkers JOIN SegmentGeneral without shared hash table.
			 * And the join locus is SegmentGeneralWorkers.
			 * Then we can return the outer locus as join will set workers as outer locus.
			 * See ex 5_4_5 in cbdb_parallel.sql
			 */
			if (outer.ok_to_replicate && inner.ok_to_replicate)
				return outer.locus;
			goto fail;
		}

		if (CdbPathLocus_IsBottleneck(inner.locus))
		{
			/*
			 * We may win if we are a parallel-aware join, SingleQE is on the inner side that
			 * means there is a chance to generate a parallel join under SingleQE.
			 * In this case, we have both side parallel and may benefit.
			 * See ex 5_P_2_2 in gp_parallel.sql
			 * If not parallel-aware, we are not sure for the benefit and a simgle test
			 * shows lower performance, ex: parallel scan on replicated table and join with
			 * SingleQE which is a non-parallel plan.
			 */
			if (parallel_aware)
			{
				segGeneral->move_to = inner.locus;
				segGeneral->move_to.numsegments = inner.locus.numsegments;
			}
			else
				goto fail;
		}
		else if (CdbPathLocus_IsPartitioned(inner.locus))
		{
			if (CdbPathLocus_NumSegments(outer.locus) != CdbPathLocus_NumSegments(inner.locus))
				goto fail;

			if (!segGeneral->ok_to_replicate)
			{
				if (!try_redistribute(root, segGeneral,
									  other, redistribution_clauses, parallel_aware))
				{
					if (parallel_aware)
						goto fail;

					CdbPathLocus_MakeSingleQE(&segGeneral->move_to,
											  CdbPathLocus_NumSegments(segGeneral->locus));
					CdbPathLocus_MakeSingleQE(&other->move_to,
											  CdbPathLocus_NumSegments(other->locus));
				}
			}
			else
			{
				/* Parallel HashedOJ is not supported yet */
				if (CdbPathLocus_IsHashedOJ(other->locus))
					goto fail;

				if (parallel_aware)
				{
					if (innerParallel != outerParallel)
						goto fail;
					/*
					 * SegmentGeneralWorkers join HashedWorkers, Hashed, Strewn when parallel_aware.
					 * Let cdbpathlocus_parallel_join decide the join locus.
					 * That will generate:
					 *  SegmentGeneralWorkers join HashedWorkers generate HashedWorkers(ex 5_P_12_12).
					 *  SegmentGeneralWorkers join Hashed generate HashedWorkers(Need to create a case).
					 *  SegmentGeneralWorkers join Strewn generate Strewn(ex 5_P_11_11).
					 */
					return cdbpathlocus_parallel_join(jointype, segGeneral->locus, other->locus, true);
				}
				else if (innerParallel == 0 && other->path->pathtype == T_SeqScan)
				{
					/*
					 * CBDB_PARALLEL_FIXME: The inner path will be duplicately processed.
					 * That require inner path should not have descendant Motion paths.
					 * Use Seqscan here is more strit, but for now.
					 *
					 * SegmentGeneralWokrers(w=N) join inner_locus(w=0).
					 * That will generate:
					 *  SegmentGeneralWorkers(w=N) join Hashed(w=0) generate HashedWorkers(w=N)(ex 5_9_12). 
					 *  SegmentGeneralWorkers(w=N) join Strewn(w=0) generate Strewn(w=N)(ex 5_11_11).
					 */
					return cdbpathlocus_parallel_join(jointype, segGeneral->locus, other->locus, false);
				}
				else
				{
					goto fail;
				}
			}
		}
	}
	/* SegmentGeneral join others */
	else if (CdbPathLocus_IsSegmentGeneral(outer.locus))
	{
		/*
		 * In principle, we couldn't get here as:
		 * 1.If both's parallel_workers is 0, they should be handled in cdbpath_motion_for_join().
		 * 2.If inner path's parallel_workers > 0, it must be from a partial_pathlist.
		 *  SegmentGeneral neighter could be from base rel's partial_pathlist nor could be from
		 *  partial_pathlist of a join locus.
		 */
		Assert(false);
		goto fail;
	}
	else if (CdbPathLocus_IsSingleQE(outer.locus))
	{
		CdbpathMfjRel *single = &outer;
		CdbpathMfjRel *other = &inner;
		bool		single_immovable = (outer.require_existing_order &&
										!outer_pathkeys) || outer.has_wts;
		bool		other_immovable = inner.require_existing_order &&
		!inner_pathkeys;

		/* single_immovable used with partitioned locus in parallel mode. */

		Assert(innerParallel != 0);
		if (innerParallel == 0)
			goto fail;

		if (CdbPathLocus_IsSegmentGeneralWorkers(other->locus))
		{
			/*
			 * We may win here if gather to SingleQE no matter what parallel-aware is.
			 * SingleQE is outer side, there could be a parallel plan under it.
			 * So we may benefit even without a shared hash table.
			 * Let the planner decide.
			 * See ex 2_P_5_2 in gp_parallel.sql.
			 */
			other->move_to = outer.locus;
			other->move_to.numsegments = outer.locus.numsegments;
		}
		else if (CdbPathLocus_IsPartitioned(other->locus))
		{
			/* If the bottlenecked rel can't be moved, bring the other rel to it. */
			if (single_immovable)
			{
				if (other_immovable)
					goto fail;
				else
					other->move_to = single->locus;
			}
			/* Redistribute single rel if joining on other rel's partitioning key */
			else if (cdbpath_match_preds_to_distkey(root,
													redistribution_clauses,
													other->path,
													other->locus,
													single->locus,
													parallel_aware,
													&single->move_to))	/* OUT */
			{
				AssertEquivalent(CdbPathLocus_NumSegments(other->locus),
								 CdbPathLocus_NumSegments(single->move_to));
			}
			/* Replicate single rel if cheaper than redistributing both rels. */
			/* CBDB_PARALLEL_FIXME: Should move to ReplicatedWorkers if parallel_aware */
			else if (single->ok_to_replicate &&
					 (single->bytes * CdbPathLocus_NumSegments(other->locus) <
					  single->bytes + other->bytes))
					CdbPathLocus_MakeReplicated(&single->move_to,
											CdbPathLocus_NumSegments(other->locus),
											single->path->parallel_workers);
			/*
			 * Redistribute both rels on equijoin cols.
			 *
			 * Redistribute both to the same segments, here we choose the
			 * same segments with other.
			 */
			else if (!other_immovable &&
					 cdbpath_distkeys_from_preds(root,
												 redistribution_clauses,
												 single->path,
												 CdbPathLocus_NumSegments(other->locus),
												 Max(single->path->parallel_workers, other->path->parallel_workers),
												 parallel_aware,
												 &single->move_to,	/* OUT */
												 &other->move_to))	/* OUT */
			{
				/* ok */
			}
			/* Broadcast single rel for below cases. */
			/* CBDB_PARALLEL_FIXME: Should move to ReplicatedWorkers if parallel_aware */
			else if (single->ok_to_replicate &&
					 (other_immovable ||
					  single->bytes < other->bytes ||
					  other->has_wts))
				CdbPathLocus_MakeReplicated(&single->move_to,
											CdbPathLocus_NumSegments(other->locus),
											single->path->parallel_workers);
			/* Last resort: If possible, move all partitions of other rel to single QE. */
			else if (!other_immovable)
				other->move_to = single->locus;
			else
				goto fail;
		}
		else
		{
			/* Should not get here. */
			Assert(false);
			goto fail;
		}
	}
	else if (CdbPathLocus_IsPartitioned(outer.locus) && (!CdbPathLocus_IsPartitioned(inner.locus)))
	{
		/*
		 * This branch handles outer Partitioned join with inner(non-Partitioned) locus.
		 * Case both are partitioned should be handled below like cbdpath_motion_for_join().
		 */
		if (CdbPathLocus_IsSegmentGeneral(inner.locus))
		{
			Assert(outerParallel > 1);
			if (!inner.ok_to_replicate)
			{
				if (!try_redistribute(root, &inner,
									  &outer, redistribution_clauses, parallel_aware))
				{
					/*
					 * CBDB_PARALLEL_FIXME:
					 * Do we need to test movable?
					 * Could we allow parallel-aware here?
					 */
					if (parallel_aware)
						goto fail;

					CdbPathLocus_MakeSingleQE(&(&inner)->move_to,
											  CdbPathLocus_NumSegments(inner.locus));
					CdbPathLocus_MakeSingleQE(&(&outer)->move_to,
											  CdbPathLocus_NumSegments(outer.locus));
				}
			}
			else
			{
				/* CBDB_PARALLEL_FIXME: redistribute to partitioned? */
				if (parallel_aware)
					goto fail;
				if (CdbPathLocus_NumSegments(inner.locus) != CdbPathLocus_NumSegments(outer.locus))
					goto fail;
				return outer.locus; /* Partitioned(workers>1) JOIN SegmentGeneral */
			}
		}
		else if (CdbPathLocus_IsSegmentGeneralWorkers(inner.locus))
		{
			/* We don't handle parallel when expanding segments */
			if (CdbPathLocus_NumSegments(outer.locus) != CdbPathLocus_NumSegments(inner.locus))
				goto fail;

			if (!inner.ok_to_replicate)
			{
				if (!try_redistribute(root, &inner,
									  &outer, redistribution_clauses, parallel_aware))
				{
					/*
					 * CBDB_PARALLEL_FIXME:
					 * Do we need to test movable?
					 * Could we allow parallel-aware here?
					 */
					if (parallel_aware)
						goto fail;

					CdbPathLocus_MakeSingleQE(&(&inner)->move_to,
											  CdbPathLocus_NumSegments(inner.locus));
					CdbPathLocus_MakeSingleQE(&(&outer)->move_to,
											  CdbPathLocus_NumSegments(outer.locus));
				}
			}
			else
			{
				if (parallel_aware)
				{
					/* CBDB_PARALLEL_FIXME: Motion(N) from one segment to M Partitioned? */
					if (innerParallel != outerParallel)
						goto fail;

					/*
					 * HashedWorkers, Hashed, Strewn JOIN SegmentGeneralWorkers with shared hash table,
					 * return the other locus anyway.
					 * see ex 11_P_5_11, ex 12_P_5_12
					 */
					return outer.locus;
				}

				/*
				 * No shared hash table join:
				 * couldn't join if other is at outer side without shared hash table.
				 * CBDB_PARALLEL_FIXME:
				 * Could we benefit from Motion(N) from one segment to M Partitioned or Gather all to single?
				 */
				goto fail;
			}

		}
		else if (CdbPathLocus_IsSingleQE(inner.locus))
		{
			CdbpathMfjRel *single = &inner;
			CdbpathMfjRel *other = &outer;
			bool	single_immovable = inner.require_existing_order && !inner_pathkeys;
			bool	other_immovable = (outer.require_existing_order && !outer_pathkeys) || outer.has_wts;
			Assert(outerParallel > 1);

			/* If the bottlenecked rel can't be moved, bring the other rel to it. */
			if (single_immovable)
			{
				if (other_immovable)
					goto fail;
				else
					other->move_to = single->locus;
			}
			/* Redistribute single rel if joining on other rel's partitioning key */
			else if (cdbpath_match_preds_to_distkey(root,
													redistribution_clauses,
													other->path,
													other->locus,
													single->locus,
													parallel_aware,
													&single->move_to))	/* OUT */
			{
				AssertEquivalent(CdbPathLocus_NumSegments(other->locus),
								CdbPathLocus_NumSegments(single->move_to));
			}
			/* Replicate single rel if cheaper than redistributing both rels. */
			else if (single->ok_to_replicate &&
					 (single->bytes * CdbPathLocus_NumSegments(other->locus) <
					  single->bytes + other->bytes))
				/* CBDB_PARALLEL_FIXME: make ReplicatedWorkers when parallel-aware */
				CdbPathLocus_MakeReplicated(&single->move_to,
										CdbPathLocus_NumSegments(other->locus),
										single->path->parallel_workers);
			/*
			 * Redistribute both rels on equijoin cols.
			 *
			 * Redistribute both to the same segments, here we choose the
			 * same segments with other.
			 */
			else if (!other_immovable &&
					 cdbpath_distkeys_from_preds(root,
												 redistribution_clauses,
												 single->path,
												 CdbPathLocus_NumSegments(other->locus),
												 Max(single->path->parallel_workers, other->path->parallel_workers),
												 parallel_aware,
												 &single->move_to,	/* OUT */
												 &other->move_to))	/* OUT */
			{
				/* ok */
			}
			/* Broadcast single rel for below cases. */
			else if (single->ok_to_replicate &&
					 (other_immovable ||
					  single->bytes < other->bytes ||
					  other->has_wts))
				/* CBDB_PARALLEL_FIXME: make ReplicatedWorkers when parallel-aware */
				CdbPathLocus_MakeReplicated(&single->move_to,
											CdbPathLocus_NumSegments(other->locus),
											single->path->parallel_workers);
			/* Last resort: If possible, move all partitions of other rel to single QE. */
			else if (!other_immovable)
				other->move_to = single->locus;
			else
				goto fail;
		}
		else
		{
			/* Should not get here. */
			Assert(false);
			goto fail;
		}
	}
	/*
	 * No motion if partitioned alike and joining on the partitioning keys.
	 */
	else if (cdbpath_match_preds_to_both_distkeys(root, redistribution_clauses,
												  outer.locus, inner.locus, parallel_aware))
		return cdbpathlocus_parallel_join(jointype, outer.locus, inner.locus, parallel_aware);

	/*
	 * Both sources are partitioned.  Redistribute or replicate one or both.
	 */
	else
	{							/* partitioned */
		CdbpathMfjRel *large_rel = &outer;
		CdbpathMfjRel *small_rel = &inner;
		int lp; /* larger rel parallel workers */
		int sp; /* small rel parallel workers */
		
		/* Consider locus when parallel_ware. */
		if(parallel_aware)
		{
			/* can't parallel join if both are Hashed, it should be in non-parallel path */
			if (CdbPathLocus_IsHashed(outer.locus) &&
				CdbPathLocus_IsHashed(inner.locus))
				goto fail;
		}

		/* Which rel is bigger? */
		/* CBDB_PARALLEL_FIXME: should we swap if parallel_aware? */
		if (large_rel->bytes < small_rel->bytes)
			CdbSwap(CdbpathMfjRel *, large_rel, small_rel);

		lp = CdbPathLocus_NumParallelWorkers(large_rel->locus);
		sp = CdbPathLocus_NumParallelWorkers(small_rel->locus);

		/* Both side are distribued in 1 segment and no parallel, it can join without motion. */
		if (CdbPathLocus_NumSegments(large_rel->locus) == 1 &&
			CdbPathLocus_NumSegments(small_rel->locus) == 1 &&
			CdbPathLocus_NumParallelWorkers(large_rel->locus) == 0 &&
			CdbPathLocus_NumParallelWorkers(small_rel->locus) == 0)
				return large_rel->locus;

		/* If joining on larger rel's partitioning key, redistribute smaller. */
		if (!small_rel->require_existing_order &&
			cdbpath_match_preds_to_distkey(root,
										   redistribution_clauses,
										   large_rel->path,
										   large_rel->locus,
										   small_rel->locus,
										   parallel_aware,
										   &small_rel->move_to))	/* OUT */
		{
			AssertEquivalent(CdbPathLocus_NumSegments(large_rel->locus),
							 CdbPathLocus_NumSegments(small_rel->move_to));
		}

		/*
		 * Replicate smaller rel if cheaper than redistributing larger rel.
		 * But don't replicate a rel that is to be preserved in outer join.
		 */
		else if (!small_rel->require_existing_order &&
				 small_rel->ok_to_replicate &&
				 ((!parallel_aware && (small_rel->bytes * CdbPathLocus_NumSegmentsPlusParallelWorkers(large_rel->locus) < large_rel->bytes)) ||
				  (parallel_aware && (small_rel->bytes * CdbPathLocus_NumSegments(large_rel->locus) < large_rel->bytes))))
				{
					if (!parallel_aware || lp <= 1)
						CdbPathLocus_MakeReplicated(&small_rel->move_to,
													CdbPathLocus_NumSegments(large_rel->locus),
													large_rel->path->parallel_workers);
					else
						CdbPathLocus_MakeReplicatedWorkers(&small_rel->move_to,
														   CdbPathLocus_NumSegments(large_rel->locus),
														   large_rel->path->parallel_workers);
				}

		/*
		 * Replicate larger rel if cheaper than redistributing smaller rel.
		 * But don't replicate a rel that is to be preserved in outer join.
		 */
		else if (!large_rel->require_existing_order &&
				 large_rel->ok_to_replicate &&
				 ((!parallel_aware && (large_rel->bytes * CdbPathLocus_NumSegmentsPlusParallelWorkers(small_rel->locus) < small_rel->bytes)) ||
				  (parallel_aware && (large_rel->bytes * CdbPathLocus_NumSegments(small_rel->locus) < small_rel->bytes))))
				{
					if (!parallel_aware || sp <= 1)
						CdbPathLocus_MakeReplicated(&large_rel->move_to,
									    			CdbPathLocus_NumSegments(small_rel->locus),
									    			small_rel->path->parallel_workers);
					else
						CdbPathLocus_MakeReplicatedWorkers(&large_rel->move_to,
									    			CdbPathLocus_NumSegments(small_rel->locus),
									    			small_rel->path->parallel_workers);
				}

		/* If joining on smaller rel's partitioning key, redistribute larger. */
		else if (!large_rel->require_existing_order &&
				(!(large_rel->path->parallel_workers > 0) || parallel_aware) &&
				 cdbpath_match_preds_to_distkey(root,
												redistribution_clauses,
												small_rel->path,
												small_rel->locus,
												large_rel->locus,
												parallel_aware,
												&large_rel->move_to))	/* OUT */
		{
			AssertEquivalent(CdbPathLocus_NumSegments(small_rel->locus),
							 CdbPathLocus_NumSegments(large_rel->move_to));
		}

		/* Replicate smaller rel if cheaper than redistributing both rels. */
		else if (!small_rel->require_existing_order &&
				 small_rel->ok_to_replicate &&
				 ((!parallel_aware && (small_rel->bytes * CdbPathLocus_NumSegmentsPlusParallelWorkers(large_rel->locus) < small_rel->bytes + large_rel->bytes)) ||
					(parallel_aware && (small_rel->bytes * CdbPathLocus_NumSegments(large_rel->locus) < small_rel->bytes + large_rel->bytes))))
				{
					if (!parallel_aware || lp <= 1)
						CdbPathLocus_MakeReplicated(&small_rel->move_to,
													CdbPathLocus_NumSegments(large_rel->locus),
													large_rel->path->parallel_workers);
					else
						CdbPathLocus_MakeReplicatedWorkers(&small_rel->move_to,
														   CdbPathLocus_NumSegments(large_rel->locus),
														   large_rel->path->parallel_workers);
				}

		/* Replicate larger rel if cheaper than redistributing both rels. */
				else if (!large_rel->require_existing_order &&
						 large_rel->ok_to_replicate &&
						 ((!parallel_aware && (large_rel->bytes * CdbPathLocus_NumSegmentsPlusParallelWorkers(small_rel->locus) < small_rel->bytes + large_rel->bytes)) ||
						  (parallel_aware && (large_rel->bytes * CdbPathLocus_NumSegments(small_rel->locus) < small_rel->bytes + large_rel->bytes))))
				{
					if (!parallel_aware || sp <= 1)
						CdbPathLocus_MakeReplicated(&large_rel->move_to,
									    			CdbPathLocus_NumSegments(small_rel->locus),
									    			small_rel->path->parallel_workers);
					else
						CdbPathLocus_MakeReplicatedWorkers(&large_rel->move_to,
									    			CdbPathLocus_NumSegments(small_rel->locus),
									    			small_rel->path->parallel_workers);
				}

		/*
		 * Redistribute both rels on equijoin cols.
		 *
		 * the two results should all be distributed on the same segments,
		 * here we make them the same with common segments for safe
		 * TODO: how about distribute them both to ALL segments?
		 */
		else if (!small_rel->require_existing_order &&
				 !small_rel->has_wts &&
				 !large_rel->require_existing_order &&
				 !large_rel->has_wts &&
				 cdbpath_distkeys_from_preds(root,
											 redistribution_clauses,
											 large_rel->path,
											 CdbPathLocus_CommonSegments(large_rel->locus,
																		 small_rel->locus),
											 Max(large_rel->path->parallel_workers, small_rel->path->parallel_workers),
											 parallel_aware,
											 &large_rel->move_to,
											 &small_rel->move_to))
		{
			/* ok */
		}

		/*
		 * No usable equijoin preds, or couldn't consider the preferred
		 * motion. Replicate one rel if possible. MPP TODO: Consider number of
		 * seg dbs per host.
		 */
		else if (!small_rel->require_existing_order &&
				 small_rel->ok_to_replicate)
				 {
					if (!parallel_aware || lp <= 1)
						CdbPathLocus_MakeReplicated(&small_rel->move_to,
												CdbPathLocus_NumSegments(large_rel->locus),
									    			large_rel->path->parallel_workers);
					else
						CdbPathLocus_MakeReplicatedWorkers(&small_rel->move_to,
									    			CdbPathLocus_NumSegments(large_rel->locus),
									    			large_rel->path->parallel_workers);
				 }

		else if (!large_rel->require_existing_order &&
				 large_rel->ok_to_replicate)
				 {
					if (!parallel_aware || sp <= 1)
						CdbPathLocus_MakeReplicated(&large_rel->move_to,
									    			CdbPathLocus_NumSegments(small_rel->locus),
									    			small_rel->path->parallel_workers);
					else
						CdbPathLocus_MakeReplicatedWorkers(&large_rel->move_to,
									    			CdbPathLocus_NumSegments(small_rel->locus),
									    			small_rel->path->parallel_workers);
				 }

		/* Last resort: Move both rels to a single qExec. */
		else
		{
			int numsegments = CdbPathLocus_CommonSegments(outer.locus,
														  inner.locus);
			CdbPathLocus_MakeSingleQE(&outer.move_to, numsegments);
			CdbPathLocus_MakeSingleQE(&inner.move_to, numsegments);
		}
	}							/* partitioned */

	/*
	 * Move outer.
	 */
	if (!CdbPathLocus_IsNull(outer.move_to))
	{
		outer.path = cdbpath_create_motion_path(root,
												outer.path,
												outer_pathkeys,
												outer.require_existing_order,
												outer.move_to);
		if (!outer.path)		/* fail if outer motion not feasible */
			goto fail;

		if (IsA(outer.path, MaterialPath) && !root->config->may_rescan)
		{
			/*
			 * If we are the outer path and can never be rescanned,
			 * we could remove the materialize path.
			 */
			MaterialPath *mpath = (MaterialPath *) outer.path;
			outer.path = mpath->subpath;
		}
	}

	/*
	 * Move inner.
	 */
	if (!CdbPathLocus_IsNull(inner.move_to))
	{
		inner.path = cdbpath_create_motion_path(root,
												inner.path,
												inner_pathkeys,
												inner.require_existing_order,
												inner.move_to);
		if (!inner.path)		/* fail if inner motion not feasible */
			goto fail;

		if (parallel_aware)
			inner.path->motionHazard = true;
	}

	/*
	 * Ok to join.  Give modified subpaths to caller.
	 */
	*p_outer_path = outer.path;
	*p_inner_path = inner.path;

	/* Tell caller where the join will be done. */
	return cdbpathlocus_parallel_join(jointype, outer.path->locus, inner.path->locus, parallel_aware);

fail:							/* can't do this join */
	CdbPathLocus_MakeNull(&outer.move_to);
	return outer.move_to;
}								/* cdbpath_motion_for_parallel_join */

void
unset_allow_append_initplan_for_function_scan()
{
	allow_append_initplan_for_function_scan = false;
}

void
set_allow_append_initplan_for_function_scan()
{
	allow_append_initplan_for_function_scan = true;
}

bool
get_allow_append_initplan_for_function_scan()
{
	return allow_append_initplan_for_function_scan;
}
