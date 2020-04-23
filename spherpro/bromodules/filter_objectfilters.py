"""
A class to generate and add filters to the filter table.
"""
import pandas as pd
import sqlalchemy as sa

import spherpro.bromodules.filter_base as filter_base
import spherpro.db as db

FILTERSTACKNAME = "FilterStack"
FILTERTYPENAME = "filter"


class ObjectFilterLib(filter_base.BaseFilter):
    def __init__(self, bro):
        super().__init__(bro)

    def _get_valueless_table(self):
        """
        Used to create a Valueless Table. The Table represents a db.object_measurements
        table, missing the value, MEasurementType, MeasurementName and PlaneID-columns
        """
        q = self.data.main_session.query(db.objects)
        objects = pd.read_sql_query(q.statement, self.data.db_conn)
        objects[db.stacks.stack_name.key] = FILTERSTACKNAME
        return objects

    def add_filtername(self, filtername):
        """
        Adds a filtername to the objectfilter
        Args:
            filtername: a string
        """
        fil_id = (self.data.main_session.query(db.object_filter_names.object_filter_id)
                  .filter(db.object_filter_names.object_filter_name == filtername).scalar())
        if fil_id is None:
            new_id = self.data._query_new_ids(db.object_filter_names.object_filter_id, 1)
            new_id = list(new_id)
            fil_id = new_id[0]
            dat = pd.DataFrame({db.object_filter_names.object_filter_name.key: [filtername],
                                db.object_filter_names.object_filter_id.key: new_id})
            self.data._add_generic_tuple(dat, db.object_filter_names)

        fil_id = int(fil_id)

        return fil_id

    def write_filter_to_db(self, filterdata, filtername, drop=True, replace=True):
        """
        Writes a dataframe containing Filterdata to the DB.
        Args:
            filterdata: DataFrame containing the filterdata. Needs to contain a column filter_value
                and object_id
            filtername: String stating the Filtername
        """
        filterdata = filterdata.loc[:, [db.objects.object_id.key, db.object_filters.filter_value.key]]
        filterdata.loc[:, db.object_filter_names.object_filter_id.key] = [self.add_filtername(filtername)]

        if drop:
            self.delete_filter_by_name(filtername)
        filterdata = filterdata.dropna()
        filterdata[db.object_filters.filter_value.key] = filterdata[db.object_filters.filter_value.key].astype(int)
        self.data._bulkinsert(filterdata, db.object_filters)

    def delete_filter_by_name(self, filtername):
        fid = (self.session.query(db.object_filter_names.object_filter_id)
               .filter(db.object_filter_names.object_filter_name == filtername)).one()[0]
        (self.session.query(db.object_filters)
         .filter(db.object_filters.object_filter_id == fid)
         ).delete()
        self.session.commit()

    def get_combined_filterquery(self, object_filters):
        """
        Get a filter query for the requested filters:
        Args:
            object_filters: list of format [(filtername1, filtervalue1),
                                            (filtername2, filtervalue2), ... ]
            image_filters: list of same format as object_filters
        returns: a subquery that can be joined to another query
        """

        subquerys = [self.data.main_session.query(db.object_filters.object_id)
                         .join(db.object_filter_names)
                         .filter(db.object_filter_names.object_filter_name == filname)
                         .filter(db.object_filters.filter_value == int(filval))
                         .subquery(filname + str(filval))
                     for filname, filval in object_filters]
        query = self.data.main_session.query(db.objects.object_id)
        for sq in subquerys:
            query = query.join(sq, db.objects.object_id == sq.c.object_id)
        return query.subquery()

    def get_combined_filterstatement(self, object_filters):
        """
        Get a filter statement for the requested filters:
        Args:
            object_filters: list of format [(filtername1, filtervalue1),
                                            (filtername2, filtervalue2), ... ]
        returns: a subquery that can be joined to another query
        """
        subquery = self.get_combined_filterquery(object_filters)
        fil = sa.and_(db.objects.object_id == subquery.c.object_id)
        return fil

    def delete_by_filter(self, filter_statement):
        """
        Deletes objects from the 'valid_objects' list based
        on an object filter statement.

        Args:
            filter_statement: an object filter statement

        Returns:

        """
        stmt = self.session.query(db.valid_objects).filter(db.valid_objects.object_id.in_(filter_statement))
        stmt.delete(synchronize_session='fetch')
        self.session.commit()
