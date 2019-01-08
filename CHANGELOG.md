## Change Log

* **master**
    * Added `field.rows` and `index.group_by` calls.

* **v1.2.0** (2018-12-21)
    * **Compatible with Pilosa 1.2**
    * Added `track_existence` index option.
    * Added `not_` index method to support `Not` queries. The corresponding index must be created with `track_existence=True`.
    * Added mutex and bool fields.
    * Added `field.clear_row`, `field.store` and `index.options` calls.
    * Added support for roaring importing `RowIDColumnID` with timestamp data.
    * Added support for clear imports. Pass `clear=True` to `client.import_field` to use it.
    * Disabled support for PyPy. Let us know if you depend on it.

* **v1.0.3** (2018-09-12)
    * Compatible with Pilosa 1.0.
    * Added key import support.

* **v1.0.0** (2018-06-28)
    * Compatible with Pilosa 1.0.
    * Added `shards` option to `client.query` which allows limiting a query to be run for the specified shards.
    * Removed all deprecated code.
    * Following terminology was changed:
        * frame to field
        * bitmap to row
        * bit to column
        * slice to shard
    * Removed `Field` type and renamed `Frame` to `Field`.
    * There are three types of fields:
        * Set fields to store boolean values (default)
        * Integer fields to store an integer in the given range.
        * Time fields which can store timestamps.

* **v0.9.0** (2018-05-08)
    * Compatible with Pilosa 0.9.
    * Added `RangeField.min` and `RangeField.max` methods.
    * **Deprecation** `inverse_enabled` frame option, `Frame.inverse_bitmap`, `Frame.inverse_topn`, `Frame.inverse_range` methods. Inverse frames will be removed on Pilosa 1.0.


* **v0.8.3** (2017-12-28):
    * Compatible with Pilosa master, **not compatible with Pilosa 0.8.x releases**.
    * Checks the server version. You can pass `skip_version_check=True` to `Client` to disable that.
    * `bitmap`, `inverse_bitmap`, `clearbit`, `range` and `set_row_attrs` methods accept string row/column keys.
    * **Removed** Row and column labels.
    * **Removed** Index options.


* **v0.8.2** (2017-12-06):
    * This release fixes the PyPI page of this library. Otherwise, it's the same as v0.8.1.

* **v0.8.1** (2017-12-06):
    * Added `equals`, `not_equals` and `not_null` field operations.
    * **Deprecation** Passing `time_quantum` to indexes. Use `time_quantum` of individual frames instead.

* **v0.8.0** (2017-11-16):
    * Added TLS support. In order to activate it, prefix the server address with `https://`.
    * IPv6 support.

* **v0.7.0** (2017-10-04):
    * Added support for creating range encoded frames.
    * Added `Xor` call.
    * Added support for excluding bits or attributes from bitmap calls.
    * Added range field operations.
    * Customizable CSV timestamp format (Contributed by @lachlanorr).
    * **Deprecation** Row and column labels are deprecated, and will be removed in a future release of this library. Do not use `column_label` field when creating `Index` objects and do not use `row_label` field when creating `Frame` objects for new code. See: https://github.com/pilosa/pilosa/issues/752 for more info.

* **v0.5.0** (2017-08-03):
    * Supports importing data to Pilosa server.
    * Failover for connection errors.
    * More logging.
    * Introduced schemas. No need to re-define already existing indexes and frames.
    * *make* commands are supported on Windows.
    * *Breaking Change* Removed `time_quantum` query option.
    * **Deprecation** `Index` constructor. Use `schema.index` instead.
    * **Deprecation** `client.create_index`, `client.create_frame`, `client.ensure_index`, `client.ensure_frame`. Use schemas and `client.sync_schema` instead.

* **v0.4.0** (2017-06-08):
    * Supports Pilosa Server v0.4.0.
    * This version has the updated documentation.
    * Some light refactoring which shouldn't affect any user code.
    * Updated the accepted values for index, frame names and labels to match with the Pilosa server.
    * `Union` queries accept 0 or more arguments. `Intersect` and `Difference` queries accept 1 or more arguments.
    * Added `inverse TopN` and `inverse Range` calls.
    * Inverse enabled status of frames is not checked on the client side.

* **v0.3.2** (2017-05-03):
    * Fixes a bug with getting the version of the package.

* **v0.3.1** (2017-05-01):
    * Initial version.
    * Supports Pilosa Server v0.3.1.
