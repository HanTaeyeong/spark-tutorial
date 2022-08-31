const test=async()=>{
    
    try{
        throw new Error('asdf')
    }catch(e){
        return e
    }

}
