def test_dict():
    mydict={}
    alist=[]
    temp=('xxx',4,5)
    temp2=('yyy',9,10)
    alist.append(temp)
    alist.append(temp2)
    blist=[('zzz',7,8),('eee',1,2)]
    mydict['f1']=alist
    mydict['f2']=blist
    #print mydict
    for mykey in mydict:
        print (mykey)
        for i in range(len(mydict[mykey])):
            (name,x,y)=mydict[mykey][i]
            print (name, x,y)

test_dict()

