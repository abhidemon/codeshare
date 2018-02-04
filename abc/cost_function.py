from sklearn import linear_model
import scipy
import numpy as np

clf = linear_model.LinearRegression()

#Cost per second
cps = [
0.0002950516975,
0.0001084777199,
0.0002169554398,
0.00005532219329,
]

'''Attributes in the order : 
    [ Variants	, Feed Size(GB),	No.of document	, Each Document Size (KB), Schema Count, Total Searchable Field]
'''
attr_orig = [
    [1,	1.2,	124626,	9.628809398,	424,	93],    # Client1
    [0,	0.032,	18729,	1.708580277,	265,	29],    # Client2
    [1,	0.042,	7008,	5.993150685,	75,	14],        # Client3
    [0,	0.089,	62868,	1.415664567,	319,	10],    # Client4
]
clients = attr_orig[::-1]

A=cps
B=clients
y = np.array(A)
X = np.array(B)

clf.fit(X, y)
coef_ = clf.coef_
print(coef_)



print "Now, trying to Verify..."
cnt = 0
for attr in attr_orig:
    val = 0
    for i in range(0, len(attr)):
        val+=attr[i] * coef_[i]
    if val-cps[cnt]>0.00001:
        print "Error"
        raise Exception("Not matching in the "+str(cnt)+"th round")
    cnt+=1






