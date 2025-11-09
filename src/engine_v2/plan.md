this version has:

Page Groups
baically in a table, the rows in each column would be divided into groups and each columnar page would have same size for those particular rows

this also simplies things as now there is only one psum array per table where one element is number of rows in a Page Group

so a page is the lowest unit of data in walrus now, we NEVER operate on a darn row anymore, so all the darn operations need to operate onn Page(s) now

how the hell do we do this now, lets delegate more shit now






