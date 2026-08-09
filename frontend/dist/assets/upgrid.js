(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const i of document.querySelectorAll('link[rel="modulepreload"]'))n(i);new MutationObserver(i=>{for(const r of i)if(r.type==="childList")for(const o of r.addedNodes)o.tagName==="LINK"&&o.rel==="modulepreload"&&n(o)}).observe(document,{childList:!0,subtree:!0});function s(i){const r={};return i.integrity&&(r.integrity=i.integrity),i.referrerPolicy&&(r.referrerPolicy=i.referrerPolicy),i.crossOrigin==="use-credentials"?r.credentials="include":i.crossOrigin==="anonymous"?r.credentials="omit":r.credentials="same-origin",r}function n(i){if(i.ep)return;i.ep=!0;const r=s(i);fetch(i.href,r)}})();const te=globalThis,Ae=te.ShadowRoot&&(te.ShadyCSS===void 0||te.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,Te=Symbol(),Re=new WeakMap;let ut=class{constructor(e,s,n){if(this._$cssResult$=!0,n!==Te)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=s}get styleSheet(){let e=this.o;const s=this.t;if(Ae&&e===void 0){const n=s!==void 0&&s.length===1;n&&(e=Re.get(s)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),n&&Re.set(s,e))}return e}toString(){return this.cssText}};const zt=t=>new ut(typeof t=="string"?t:t+"",void 0,Te),ht=(t,...e)=>{const s=t.length===1?t[0]:e.reduce((n,i,r)=>n+(o=>{if(o._$cssResult$===!0)return o.cssText;if(typeof o=="number")return o;throw Error("Value passed to 'css' function must be a 'css' function result: "+o+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(i)+t[r+1],t[0]);return new ut(s,t,Te)},Ht=(t,e)=>{if(Ae)t.adoptedStyleSheets=e.map(s=>s instanceof CSSStyleSheet?s:s.styleSheet);else for(const s of e){const n=document.createElement("style"),i=te.litNonce;i!==void 0&&n.setAttribute("nonce",i),n.textContent=s.cssText,t.appendChild(n)}},Le=Ae?t=>t:t=>t instanceof CSSStyleSheet?(e=>{let s="";for(const n of e.cssRules)s+=n.cssText;return zt(s)})(t):t;const{is:Jt,defineProperty:Bt,getOwnPropertyDescriptor:Vt,getOwnPropertyNames:Wt,getOwnPropertySymbols:Kt,getPrototypeOf:Qt}=Object,de=globalThis,Ue=de.trustedTypes,Gt=Ue?Ue.emptyScript:"",Yt=de.reactiveElementPolyfillSupport,J=(t,e)=>t,re={toAttribute(t,e){switch(e){case Boolean:t=t?Gt:null;break;case Object:case Array:t=t==null?t:JSON.stringify(t)}return t},fromAttribute(t,e){let s=t;switch(e){case Boolean:s=t!==null;break;case Number:s=t===null?null:Number(t);break;case Object:case Array:try{s=JSON.parse(t)}catch{s=null}}return s}},Ee=(t,e)=>!Jt(t,e),Fe={attribute:!0,type:String,converter:re,reflect:!1,useDefault:!1,hasChanged:Ee};Symbol.metadata??=Symbol("metadata"),de.litPropertyMetadata??=new WeakMap;let N=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,s=Fe){if(s.state&&(s.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((s=Object.create(s)).wrapped=!0),this.elementProperties.set(e,s),!s.noAccessor){const n=Symbol(),i=this.getPropertyDescriptor(e,n,s);i!==void 0&&Bt(this.prototype,e,i)}}static getPropertyDescriptor(e,s,n){const{get:i,set:r}=Vt(this.prototype,e)??{get(){return this[s]},set(o){this[s]=o}};return{get:i,set(o){const a=i?.call(this);r?.call(this,o),this.requestUpdate(e,a,n)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??Fe}static _$Ei(){if(this.hasOwnProperty(J("elementProperties")))return;const e=Qt(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(J("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(J("properties"))){const s=this.properties,n=[...Wt(s),...Kt(s)];for(const i of n)this.createProperty(i,s[i])}const e=this[Symbol.metadata];if(e!==null){const s=litPropertyMetadata.get(e);if(s!==void 0)for(const[n,i]of s)this.elementProperties.set(n,i)}this._$Eh=new Map;for(const[s,n]of this.elementProperties){const i=this._$Eu(s,n);i!==void 0&&this._$Eh.set(i,s)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){const s=[];if(Array.isArray(e)){const n=new Set(e.flat(1/0).reverse());for(const i of n)s.unshift(Le(i))}else e!==void 0&&s.push(Le(e));return s}static _$Eu(e,s){const n=s.attribute;return n===!1?void 0:typeof n=="string"?n:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){const e=new Map,s=this.constructor.elementProperties;for(const n of s.keys())this.hasOwnProperty(n)&&(e.set(n,this[n]),delete this[n]);e.size>0&&(this._$Ep=e)}createRenderRoot(){const e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return Ht(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,s,n){this._$AK(e,n)}_$ET(e,s){const n=this.constructor.elementProperties.get(e),i=this.constructor._$Eu(e,n);if(i!==void 0&&n.reflect===!0){const r=(n.converter?.toAttribute!==void 0?n.converter:re).toAttribute(s,n.type);this._$Em=e,r==null?this.removeAttribute(i):this.setAttribute(i,r),this._$Em=null}}_$AK(e,s){const n=this.constructor,i=n._$Eh.get(e);if(i!==void 0&&this._$Em!==i){const r=n.getPropertyOptions(i),o=typeof r.converter=="function"?{fromAttribute:r.converter}:r.converter?.fromAttribute!==void 0?r.converter:re;this._$Em=i;const a=o.fromAttribute(s,r.type);this[i]=a??this._$Ej?.get(i)??a,this._$Em=null}}requestUpdate(e,s,n,i=!1,r){if(e!==void 0){const o=this.constructor;if(i===!1&&(r=this[e]),n??=o.getPropertyOptions(e),!((n.hasChanged??Ee)(r,s)||n.useDefault&&n.reflect&&r===this._$Ej?.get(e)&&!this.hasAttribute(o._$Eu(e,n))))return;this.C(e,s,n)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,s,{useDefault:n,reflect:i,wrapped:r},o){n&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,o??s??this[e]),r!==!0||o!==void 0)||(this._$AL.has(e)||(this.hasUpdated||n||(s=void 0),this._$AL.set(e,s)),i===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(s){Promise.reject(s)}const e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[i,r]of this._$Ep)this[i]=r;this._$Ep=void 0}const n=this.constructor.elementProperties;if(n.size>0)for(const[i,r]of n){const{wrapped:o}=r,a=this[i];o!==!0||this._$AL.has(i)||a===void 0||this.C(i,void 0,r,a)}}let e=!1;const s=this._$AL;try{e=this.shouldUpdate(s),e?(this.willUpdate(s),this._$EO?.forEach(n=>n.hostUpdate?.()),this.update(s)):this._$EM()}catch(n){throw e=!1,this._$EM(),n}e&&this._$AE(s)}willUpdate(e){}_$AE(e){this._$EO?.forEach(s=>s.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(s=>this._$ET(s,this[s])),this._$EM()}updated(e){}firstUpdated(e){}};N.elementStyles=[],N.shadowRootOptions={mode:"open"},N[J("elementProperties")]=new Map,N[J("finalized")]=new Map,Yt?.({ReactiveElement:N}),(de.reactiveElementVersions??=[]).push("2.1.2");const Pe=globalThis,qe=t=>t,oe=Pe.trustedTypes,ze=oe?oe.createPolicy("lit-html",{createHTML:t=>t}):void 0,pt="$lit$",T=`lit$${Math.random().toFixed(9).slice(2)}$`,ft="?"+T,Zt=`<${ft}>`,D=document,V=()=>D.createComment(""),W=t=>t===null||typeof t!="object"&&typeof t!="function",Ie=Array.isArray,Xt=t=>Ie(t)||typeof t?.[Symbol.iterator]=="function",me=`[ 	
\f\r]`,q=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,He=/-->/g,Je=/>/g,O=RegExp(`>|${me}(?:([^\\s"'>=/]+)(${me}*=${me}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),Be=/'/g,Ve=/"/g,gt=/^(?:script|style|textarea|title)$/i,es=t=>(e,...s)=>({_$litType$:t,strings:e,values:s}),h=es(1),R=Symbol.for("lit-noChange"),f=Symbol.for("lit-nothing"),We=new WeakMap,j=D.createTreeWalker(D,129);function mt(t,e){if(!Ie(t)||!t.hasOwnProperty("raw"))throw Error("invalid template strings array");return ze!==void 0?ze.createHTML(e):e}const ts=(t,e)=>{const s=t.length-1,n=[];let i,r=e===2?"<svg>":e===3?"<math>":"",o=q;for(let a=0;a<s;a++){const l=t[a];let c,u,d=-1,p=0;for(;p<l.length&&(o.lastIndex=p,u=o.exec(l),u!==null);)p=o.lastIndex,o===q?u[1]==="!--"?o=He:u[1]!==void 0?o=Je:u[2]!==void 0?(gt.test(u[2])&&(i=RegExp("</"+u[2],"g")),o=O):u[3]!==void 0&&(o=O):o===O?u[0]===">"?(o=i??q,d=-1):u[1]===void 0?d=-2:(d=o.lastIndex-u[2].length,c=u[1],o=u[3]===void 0?O:u[3]==='"'?Ve:Be):o===Ve||o===Be?o=O:o===He||o===Je?o=q:(o=O,i=void 0);const y=o===O&&t[a+1].startsWith("/>")?" ":"";r+=o===q?l+Zt:d>=0?(n.push(c),l.slice(0,d)+pt+l.slice(d)+T+y):l+T+(d===-2?a:y)}return[mt(t,r+(t[s]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),n]};class K{constructor({strings:e,_$litType$:s},n){let i;this.parts=[];let r=0,o=0;const a=e.length-1,l=this.parts,[c,u]=ts(e,s);if(this.el=K.createElement(c,n),j.currentNode=this.el.content,s===2||s===3){const d=this.el.content.firstChild;d.replaceWith(...d.childNodes)}for(;(i=j.nextNode())!==null&&l.length<a;){if(i.nodeType===1){if(i.hasAttributes())for(const d of i.getAttributeNames())if(d.endsWith(pt)){const p=u[o++],y=i.getAttribute(d).split(T),$=/([.?@])?(.*)/.exec(p);l.push({type:1,index:r,name:$[2],strings:y,ctor:$[1]==="."?is:$[1]==="?"?ns:$[1]==="@"?rs:ue}),i.removeAttribute(d)}else d.startsWith(T)&&(l.push({type:6,index:r}),i.removeAttribute(d));if(gt.test(i.tagName)){const d=i.textContent.split(T),p=d.length-1;if(p>0){i.textContent=oe?oe.emptyScript:"";for(let y=0;y<p;y++)i.append(d[y],V()),j.nextNode(),l.push({type:2,index:++r});i.append(d[p],V())}}}else if(i.nodeType===8)if(i.data===ft)l.push({type:2,index:r});else{let d=-1;for(;(d=i.data.indexOf(T,d+1))!==-1;)l.push({type:7,index:r}),d+=T.length-1}r++}}static createElement(e,s){const n=D.createElement("template");return n.innerHTML=e,n}}function L(t,e,s=t,n){if(e===R)return e;let i=n!==void 0?s._$Co?.[n]:s._$Cl;const r=W(e)?void 0:e._$litDirective$;return i?.constructor!==r&&(i?._$AO?.(!1),r===void 0?i=void 0:(i=new r(t),i._$AT(t,s,n)),n!==void 0?(s._$Co??=[])[n]=i:s._$Cl=i),i!==void 0&&(e=L(t,i._$AS(t,e.values),i,n)),e}class ss{constructor(e,s){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=s}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){const{el:{content:s},parts:n}=this._$AD,i=(e?.creationScope??D).importNode(s,!0);j.currentNode=i;let r=j.nextNode(),o=0,a=0,l=n[0];for(;l!==void 0;){if(o===l.index){let c;l.type===2?c=new Y(r,r.nextSibling,this,e):l.type===1?c=new l.ctor(r,l.name,l.strings,this,e):l.type===6&&(c=new os(r,this,e)),this._$AV.push(c),l=n[++a]}o!==l?.index&&(r=j.nextNode(),o++)}return j.currentNode=D,i}p(e){let s=0;for(const n of this._$AV)n!==void 0&&(n.strings!==void 0?(n._$AI(e,n,s),s+=n.strings.length-2):n._$AI(e[s])),s++}}class Y{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,s,n,i){this.type=2,this._$AH=f,this._$AN=void 0,this._$AA=e,this._$AB=s,this._$AM=n,this.options=i,this._$Cv=i?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode;const s=this._$AM;return s!==void 0&&e?.nodeType===11&&(e=s.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,s=this){e=L(this,e,s),W(e)?e===f||e==null||e===""?(this._$AH!==f&&this._$AR(),this._$AH=f):e!==this._$AH&&e!==R&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):Xt(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==f&&W(this._$AH)?this._$AA.nextSibling.data=e:this.T(D.createTextNode(e)),this._$AH=e}$(e){const{values:s,_$litType$:n}=e,i=typeof n=="number"?this._$AC(e):(n.el===void 0&&(n.el=K.createElement(mt(n.h,n.h[0]),this.options)),n);if(this._$AH?._$AD===i)this._$AH.p(s);else{const r=new ss(i,this),o=r.u(this.options);r.p(s),this.T(o),this._$AH=r}}_$AC(e){let s=We.get(e.strings);return s===void 0&&We.set(e.strings,s=new K(e)),s}k(e){Ie(this._$AH)||(this._$AH=[],this._$AR());const s=this._$AH;let n,i=0;for(const r of e)i===s.length?s.push(n=new Y(this.O(V()),this.O(V()),this,this.options)):n=s[i],n._$AI(r),i++;i<s.length&&(this._$AR(n&&n._$AB.nextSibling,i),s.length=i)}_$AR(e=this._$AA.nextSibling,s){for(this._$AP?.(!1,!0,s);e!==this._$AB;){const n=qe(e).nextSibling;qe(e).remove(),e=n}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}}class ue{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,s,n,i,r){this.type=1,this._$AH=f,this._$AN=void 0,this.element=e,this.name=s,this._$AM=i,this.options=r,n.length>2||n[0]!==""||n[1]!==""?(this._$AH=Array(n.length-1).fill(new String),this.strings=n):this._$AH=f}_$AI(e,s=this,n,i){const r=this.strings;let o=!1;if(r===void 0)e=L(this,e,s,0),o=!W(e)||e!==this._$AH&&e!==R,o&&(this._$AH=e);else{const a=e;let l,c;for(e=r[0],l=0;l<r.length-1;l++)c=L(this,a[n+l],s,l),c===R&&(c=this._$AH[l]),o||=!W(c)||c!==this._$AH[l],c===f?e=f:e!==f&&(e+=(c??"")+r[l+1]),this._$AH[l]=c}o&&!i&&this.j(e)}j(e){e===f?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}}class is extends ue{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===f?void 0:e}}class ns extends ue{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==f)}}class rs extends ue{constructor(e,s,n,i,r){super(e,s,n,i,r),this.type=5}_$AI(e,s=this){if((e=L(this,e,s,0)??f)===R)return;const n=this._$AH,i=e===f&&n!==f||e.capture!==n.capture||e.once!==n.once||e.passive!==n.passive,r=e!==f&&(n===f||i);i&&this.element.removeEventListener(this.name,this,n),r&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}}class os{constructor(e,s,n){this.element=e,this.type=6,this._$AN=void 0,this._$AM=s,this.options=n}get _$AU(){return this._$AM._$AU}_$AI(e){L(this,e)}}const as=Pe.litHtmlPolyfillSupport;as?.(K,Y),(Pe.litHtmlVersions??=[]).push("3.3.3");const ls=(t,e,s)=>{const n=s?.renderBefore??e;let i=n._$litPart$;if(i===void 0){const r=s?.renderBefore??null;n._$litPart$=i=new Y(e.insertBefore(V(),r),r,void 0,s??{})}return i._$AI(t),i};const Oe=globalThis;class M extends N{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){const s=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=ls(s,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return R}}M._$litElement$=!0,M.finalized=!0,Oe.litElementHydrateSupport?.({LitElement:M});const cs=Oe.litElementPolyfillSupport;cs?.({LitElement:M});(Oe.litElementVersions??=[]).push("4.2.2");const bt=t=>(e,s)=>{s!==void 0?s.addInitializer(()=>{customElements.define(t,e)}):customElements.define(t,e)};const ds={attribute:!0,type:String,converter:re,reflect:!1,hasChanged:Ee},us=(t=ds,e,s)=>{const{kind:n,metadata:i}=s;let r=globalThis.litPropertyMetadata.get(i);if(r===void 0&&globalThis.litPropertyMetadata.set(i,r=new Map),n==="setter"&&((t=Object.create(t)).wrapped=!0),r.set(s.name,t),n==="accessor"){const{name:o}=s;return{set(a){const l=e.get.call(this);e.set.call(this,a),this.requestUpdate(o,l,t,!0,a)},init(a){return a!==void 0&&this.C(o,void 0,t,a),a}}}if(n==="setter"){const{name:o}=s;return function(a){const l=this[o];e.call(this,a),this.requestUpdate(o,l,t,!0,a)}}throw Error("Unsupported decorator location: "+n)};function vt(t){return(e,s)=>typeof s=="object"?us(t,e,s):((n,i,r)=>{const o=i.hasOwnProperty(r);return i.constructor.createProperty(r,n),o?Object.getOwnPropertyDescriptor(i,r):void 0})(t,e,s)}function g(t){return vt({...t,state:!0,attribute:!1})}const yt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 4h4v16H6zm8 0h4v16h-4z"/>'},xt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 3l14 9l-14 9V3z"/>'},$t={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 6L6 18M6 6l12 12"/>'};const wt=Object.freeze({left:0,top:0,width:16,height:16}),ae=Object.freeze({rotate:0,vFlip:!1,hFlip:!1}),Z=Object.freeze({...wt,...ae}),xe=Object.freeze({...Z,body:"",hidden:!1}),hs=Object.freeze({width:null,height:null}),_t=Object.freeze({...hs,...ae});function ps(t,e=0){const s=t.replace(/^-?[0-9.]*/,"");function n(i){for(;i<0;)i+=4;return i%4}if(s===""){const i=parseInt(t);return isNaN(i)?0:n(i)}else if(s!==t){let i=0;switch(s){case"%":i=25;break;case"deg":i=90}if(i){let r=parseFloat(t.slice(0,t.length-s.length));return isNaN(r)?0:(r=r/i,r%1===0?n(r):0)}}return e}const fs=/[\s,]+/;function gs(t,e){e.split(fs).forEach(s=>{switch(s.trim()){case"horizontal":t.hFlip=!0;break;case"vertical":t.vFlip=!0;break}})}const kt={..._t,preserveAspectRatio:""};function Ke(t){const e={...kt},s=(n,i)=>t.getAttribute(n)||i;return e.width=s("width",null),e.height=s("height",null),e.rotate=ps(s("rotate","")),gs(e,s("flip","")),e.preserveAspectRatio=s("preserveAspectRatio",s("preserveaspectratio","")),e}function ms(t,e){for(const s in kt)if(t[s]!==e[s])return!0;return!1}const St=/^[a-z0-9]+(-[a-z0-9]+)*$/,X=(t,e,s,n="")=>{const i=t.split(":");if(t.slice(0,1)==="@"){if(i.length<2||i.length>3)return null;n=i.shift().slice(1)}if(i.length>3||!i.length)return null;if(i.length>1){const a=i.pop(),l=i.pop(),c={provider:i.length>0?i[0]:n,prefix:l,name:a};return e&&!se(c)?null:c}const r=i[0],o=r.split("-");if(o.length>1){const a={provider:n,prefix:o.shift(),name:o.join("-")};return e&&!se(a)?null:a}if(s&&n===""){const a={provider:n,prefix:"",name:r};return e&&!se(a,s)?null:a}return null},se=(t,e)=>t?!!((e&&t.prefix===""||t.prefix)&&t.name):!1;function bs(t,e){const s=t.icons,n=t.aliases||Object.create(null),i=Object.create(null);function r(o){if(s[o])return i[o]=[];if(!(o in i)){i[o]=null;const a=n[o]&&n[o].parent,l=a&&r(a);l&&(i[o]=[a].concat(l))}return i[o]}return Object.keys(s).concat(Object.keys(n)).forEach(r),i}function vs(t,e){const s={};!t.hFlip!=!e.hFlip&&(s.hFlip=!0),!t.vFlip!=!e.vFlip&&(s.vFlip=!0);const n=((t.rotate||0)+(e.rotate||0))%4;return n&&(s.rotate=n),s}function Qe(t,e){const s=vs(t,e);for(const n in xe)n in ae?n in t&&!(n in s)&&(s[n]=ae[n]):n in e?s[n]=e[n]:n in t&&(s[n]=t[n]);return s}function ys(t,e,s){const n=t.icons,i=t.aliases||Object.create(null);let r={};function o(a){r=Qe(n[a]||i[a],r)}return o(e),s.forEach(o),Qe(t,r)}function Ct(t,e){const s=[];if(typeof t!="object"||typeof t.icons!="object")return s;t.not_found instanceof Array&&t.not_found.forEach(i=>{e(i,null),s.push(i)});const n=bs(t);for(const i in n){const r=n[i];r&&(e(i,ys(t,i,r)),s.push(i))}return s}const xs={provider:"",aliases:{},not_found:{},...wt};function be(t,e){for(const s in e)if(s in t&&typeof t[s]!=typeof e[s])return!1;return!0}function At(t){if(typeof t!="object"||t===null)return null;const e=t;if(typeof e.prefix!="string"||!t.icons||typeof t.icons!="object"||!be(t,xs))return null;const s=e.icons;for(const i in s){const r=s[i];if(!i||typeof r.body!="string"||!be(r,xe))return null}const n=e.aliases||Object.create(null);for(const i in n){const r=n[i],o=r.parent;if(!i||typeof o!="string"||!s[o]&&!n[o]||!be(r,xe))return null}return e}const le=Object.create(null);function $s(t,e){return{provider:t,prefix:e,icons:Object.create(null),missing:new Set}}function A(t,e){const s=le[t]||(le[t]=Object.create(null));return s[e]||(s[e]=$s(t,e))}function Tt(t,e){return At(e)?Ct(e,(s,n)=>{n?t.icons[s]=n:t.missing.add(s)}):[]}function ws(t,e,s){try{if(typeof s.body=="string")return t.icons[e]={...s},!0}catch{}return!1}function _s(t,e){let s=[];return(typeof t=="string"?[t]:Object.keys(le)).forEach(n=>{(typeof n=="string"&&typeof e=="string"?[e]:Object.keys(le[n]||{})).forEach(i=>{const r=A(n,i);s=s.concat(Object.keys(r.icons).map(o=>(n!==""?"@"+n+":":"")+i+":"+o))})}),s}let Q=!1;function Et(t){return typeof t=="boolean"&&(Q=t),Q}function G(t){const e=typeof t=="string"?X(t,!0,Q):t;if(e){const s=A(e.provider,e.prefix),n=e.name;return s.icons[n]||(s.missing.has(n)?null:void 0)}}function Pt(t,e){const s=X(t,!0,Q);if(!s)return!1;const n=A(s.provider,s.prefix);return e?ws(n,s.name,e):(n.missing.add(s.name),!0)}function Ge(t,e){if(typeof t!="object")return!1;if(typeof e!="string"&&(e=t.provider||""),Q&&!e&&!t.prefix){let n=!1;return At(t)&&(t.prefix="",Ct(t,(i,r)=>{Pt(i,r)&&(n=!0)})),n}const s=t.prefix;return se({prefix:s,name:"a"})?!!Tt(A(e,s),t):!1}function ks(t){return!!G(t)}function Ss(t){const e=G(t);return e&&{...Z,...e}}function It(t,e){t.forEach(s=>{const n=s.loaderCallbacks;n&&(s.loaderCallbacks=n.filter(i=>i.id!==e))})}function Cs(t){t.pendingCallbacksFlag||(t.pendingCallbacksFlag=!0,setTimeout(()=>{t.pendingCallbacksFlag=!1;const e=t.loaderCallbacks?t.loaderCallbacks.slice(0):[];if(!e.length)return;let s=!1;const n=t.provider,i=t.prefix;e.forEach(r=>{const o=r.icons,a=o.pending.length;o.pending=o.pending.filter(l=>{if(l.prefix!==i)return!0;const c=l.name;if(t.icons[c])o.loaded.push({provider:n,prefix:i,name:c});else if(t.missing.has(c))o.missing.push({provider:n,prefix:i,name:c});else return s=!0,!0;return!1}),o.pending.length!==a&&(s||It([t],r.id),r.callback(o.loaded.slice(0),o.missing.slice(0),o.pending.slice(0),r.abort))})}))}let As=0;function Ts(t,e,s){const n=As++,i=It.bind(null,s,n);if(!e.pending.length)return i;const r={id:n,icons:e,callback:t,abort:i};return s.forEach(o=>{(o.loaderCallbacks||(o.loaderCallbacks=[])).push(r)}),i}function Es(t){const e={loaded:[],missing:[],pending:[]},s=Object.create(null);t.sort((i,r)=>i.provider!==r.provider?i.provider.localeCompare(r.provider):i.prefix!==r.prefix?i.prefix.localeCompare(r.prefix):i.name.localeCompare(r.name));let n={provider:"",prefix:"",name:""};return t.forEach(i=>{if(n.name===i.name&&n.prefix===i.prefix&&n.provider===i.provider)return;n=i;const r=i.provider,o=i.prefix,a=i.name,l=s[r]||(s[r]=Object.create(null)),c=l[o]||(l[o]=A(r,o));let u;a in c.icons?u=e.loaded:o===""||c.missing.has(a)?u=e.missing:u=e.pending;const d={provider:r,prefix:o,name:a};u.push(d)}),e}const $e=Object.create(null);function Ye(t,e){$e[t]=e}function we(t){return $e[t]||$e[""]}function Ps(t,e=!0,s=!1){const n=[];return t.forEach(i=>{const r=typeof i=="string"?X(i,e,s):i;r&&n.push(r)}),n}function je(t){let e;if(typeof t.resources=="string")e=[t.resources];else if(e=t.resources,!(e instanceof Array)||!e.length)return null;return{resources:e,path:t.path||"/",maxURL:t.maxURL||500,rotate:t.rotate||750,timeout:t.timeout||5e3,random:t.random===!0,index:t.index||0,dataAfterTimeout:t.dataAfterTimeout!==!1}}const he=Object.create(null),z=["https://api.simplesvg.com","https://api.unisvg.com"],ie=[];for(;z.length>0;)z.length===1||Math.random()>.5?ie.push(z.shift()):ie.push(z.pop());he[""]=je({resources:["https://api.iconify.design"].concat(ie)});function Ze(t,e){const s=je(e);return s===null?!1:(he[t]=s,!0)}function pe(t){return he[t]}function Is(){return Object.keys(he)}const Os={resources:[],index:0,timeout:2e3,rotate:750,random:!1,dataAfterTimeout:!1};function js(t,e,s,n){const i=t.resources.length,r=t.random?Math.floor(Math.random()*i):t.index;let o;if(t.random){let m=t.resources.slice(0);for(o=[];m.length>1;){const k=Math.floor(Math.random()*m.length);o.push(m[k]),m=m.slice(0,k).concat(m.slice(k+1))}o=o.concat(m)}else o=t.resources.slice(r).concat(t.resources.slice(0,r));const a=Date.now();let l="pending",c=0,u,d=null,p=[],y=[];typeof n=="function"&&y.push(n);function $(){d&&(clearTimeout(d),d=null)}function C(){l==="pending"&&(l="aborted"),$(),p.forEach(m=>{m.status==="pending"&&(m.status="aborted")}),p=[]}function w(m,k){k&&(y=[]),typeof m=="function"&&y.push(m)}function fe(){return{startTime:a,payload:e,status:l,queriesSent:c,queriesPending:p.length,subscribe:w,abort:C}}function P(){l="failed",y.forEach(m=>{m(void 0,u)})}function S(){p.forEach(m=>{m.status==="pending"&&(m.status="aborted")}),p=[]}function _(m,k,F){const ee=k!=="success";switch(p=p.filter(I=>I!==m),l){case"pending":break;case"failed":if(ee||!t.dataAfterTimeout)return;break;default:return}if(k==="abort"){u=F,P();return}if(ee){u=F,p.length||(o.length?ge():P());return}if($(),S(),!t.random){const I=t.resources.indexOf(m.resource);I!==-1&&I!==t.index&&(t.index=I)}l="completed",y.forEach(I=>{I(F)})}function ge(){if(l!=="pending")return;$();const m=o.shift();if(m===void 0){if(p.length){d=setTimeout(()=>{$(),l==="pending"&&(S(),P())},t.timeout);return}P();return}const k={status:"pending",resource:m,callback:(F,ee)=>{_(k,F,ee)}};p.push(k),c++,d=setTimeout(ge,t.rotate),s(m,e,k.callback)}return setTimeout(ge),fe}function Ot(t){const e={...Os,...t};let s=[];function n(){s=s.filter(o=>o().status==="pending")}function i(o,a,l){const c=js(e,o,a,(u,d)=>{n(),l&&l(u,d)});return s.push(c),c}function r(o){return s.find(a=>o(a))||null}return{query:i,find:r,setIndex:o=>{e.index=o},getIndex:()=>e.index,cleanup:n}}function Xe(){}const ve=Object.create(null);function Ds(t){if(!ve[t]){const e=pe(t);if(!e)return;ve[t]={config:e,redundancy:Ot(e)}}return ve[t]}function jt(t,e,s){let n,i;if(typeof t=="string"){const r=we(t);if(!r)return s(void 0,424),Xe;i=r.send;const o=Ds(t);o&&(n=o.redundancy)}else{const r=je(t);if(r){n=Ot(r);const o=we(t.resources?t.resources[0]:"");o&&(i=o.send)}}return!n||!i?(s(void 0,424),Xe):n.query(e,i,s)().abort}function et(){}function Ns(t){t.iconsLoaderFlag||(t.iconsLoaderFlag=!0,setTimeout(()=>{t.iconsLoaderFlag=!1,Cs(t)}))}function Ms(t){const e=[],s=[];return t.forEach(n=>{(n.match(St)?e:s).push(n)}),{valid:e,invalid:s}}function H(t,e,s){function n(){const i=t.pendingIcons;e.forEach(r=>{i&&i.delete(r),t.icons[r]||t.missing.add(r)})}if(s&&typeof s=="object")try{if(!Tt(t,s).length){n();return}}catch(i){console.error(i)}n(),Ns(t)}function tt(t,e){t instanceof Promise?t.then(s=>{e(s)}).catch(()=>{e(null)}):e(t)}function Rs(t,e){t.iconsToLoad?t.iconsToLoad=t.iconsToLoad.concat(e).sort():t.iconsToLoad=e,t.iconsQueueFlag||(t.iconsQueueFlag=!0,setTimeout(()=>{t.iconsQueueFlag=!1;const{provider:s,prefix:n}=t,i=t.iconsToLoad;if(delete t.iconsToLoad,!i||!i.length)return;const r=t.loadIcon;if(t.loadIcons&&(i.length>1||!r)){tt(t.loadIcons(i,n,s),c=>{H(t,i,c)});return}if(r){i.forEach(c=>{tt(r(c,n,s),u=>{H(t,[c],u?{prefix:n,icons:{[c]:u}}:null)})});return}const{valid:o,invalid:a}=Ms(i);if(a.length&&H(t,a,null),!o.length)return;const l=n.match(St)?we(s):null;if(!l){H(t,o,null);return}l.prepare(s,n,o).forEach(c=>{jt(s,c,u=>{H(t,c.icons,u)})})}))}const De=(t,e)=>{const s=Es(Ps(t,!0,Et()));if(!s.pending.length){let a=!0;return e&&setTimeout(()=>{a&&e(s.loaded,s.missing,s.pending,et)}),()=>{a=!1}}const n=Object.create(null),i=[];let r,o;return s.pending.forEach(a=>{const{provider:l,prefix:c}=a;if(c===o&&l===r)return;r=l,o=c,i.push(A(l,c));const u=n[l]||(n[l]=Object.create(null));u[c]||(u[c]=[])}),s.pending.forEach(a=>{const{provider:l,prefix:c,name:u}=a,d=A(l,c),p=d.pendingIcons||(d.pendingIcons=new Set);p.has(u)||(p.add(u),n[l][c].push(u))}),i.forEach(a=>{const l=n[a.provider][a.prefix];l.length&&Rs(a,l)}),e?Ts(e,s,i):et},Ls=t=>new Promise((e,s)=>{const n=typeof t=="string"?X(t,!0):t;if(!n){s(t);return}De([n||t],i=>{if(i.length&&n){const r=G(n);if(r){e({...Z,...r});return}}s(t)})});function st(t){try{const e=typeof t=="string"?JSON.parse(t):t;if(typeof e.body=="string")return{...e}}catch{}}function Us(t,e){if(typeof t=="object")return{data:st(t),value:t};if(typeof t!="string")return{value:t};if(t.includes("{")){const r=st(t);if(r)return{data:r,value:t}}const s=X(t,!0,!0);if(!s)return{value:t};const n=G(s);if(n!==void 0||!s.prefix)return{value:t,name:s,data:n};const i=De([s],()=>e(t,s,G(s)));return{value:t,name:s,loading:i}}let Dt=!1;try{Dt=navigator.vendor.indexOf("Apple")===0}catch{}function Fs(t,e){switch(e){case"svg":case"bg":case"mask":return e}return e!=="style"&&(Dt||t.indexOf("<a")===-1)?"svg":t.indexOf("currentColor")===-1?"bg":"mask"}const qs=/(-?[0-9.]*[0-9]+[0-9.]*)/g,zs=/^-?[0-9.]*[0-9]+[0-9.]*$/g;function _e(t,e,s){if(e===1)return t;if(s=s||100,typeof t=="number")return Math.ceil(t*e*s)/s;if(typeof t!="string")return t;const n=t.split(qs);if(n===null||!n.length)return t;const i=[];let r=n.shift(),o=zs.test(r);for(;;){if(o){const a=parseFloat(r);isNaN(a)?i.push(r):i.push(Math.ceil(a*e*s)/s)}else i.push(r);if(r=n.shift(),r===void 0)return i.join("");o=!o}}function Hs(t,e="defs"){let s="";const n=t.indexOf("<"+e);for(;n>=0;){const i=t.indexOf(">",n),r=t.indexOf("</"+e);if(i===-1||r===-1)break;const o=t.indexOf(">",r);if(o===-1)break;s+=t.slice(i+1,r).trim(),t=t.slice(0,n).trim()+t.slice(o+1)}return{defs:s,content:t}}function Js(t,e){return t?"<defs>"+t+"</defs>"+e:e}function Bs(t,e,s){const n=Hs(t);return Js(n.defs,e+n.content+s)}const Vs=t=>t==="unset"||t==="undefined"||t==="none";function Nt(t,e){const s={...Z,...t},n={..._t,...e},i={left:s.left,top:s.top,width:s.width,height:s.height};let r=s.body;[s,n].forEach(C=>{const w=[],fe=C.hFlip,P=C.vFlip;let S=C.rotate;fe?P?S+=2:(w.push("translate("+(i.width+i.left).toString()+" "+(0-i.top).toString()+")"),w.push("scale(-1 1)"),i.top=i.left=0):P&&(w.push("translate("+(0-i.left).toString()+" "+(i.height+i.top).toString()+")"),w.push("scale(1 -1)"),i.top=i.left=0);let _;switch(S<0&&(S-=Math.floor(S/4)*4),S=S%4,S){case 1:_=i.height/2+i.top,w.unshift("rotate(90 "+_.toString()+" "+_.toString()+")");break;case 2:w.unshift("rotate(180 "+(i.width/2+i.left).toString()+" "+(i.height/2+i.top).toString()+")");break;case 3:_=i.width/2+i.left,w.unshift("rotate(-90 "+_.toString()+" "+_.toString()+")");break}S%2===1&&(i.left!==i.top&&(_=i.left,i.left=i.top,i.top=_),i.width!==i.height&&(_=i.width,i.width=i.height,i.height=_)),w.length&&(r=Bs(r,'<g transform="'+w.join(" ")+'">',"</g>"))});const o=n.width,a=n.height,l=i.width,c=i.height;let u,d;o===null?(d=a===null?"1em":a==="auto"?c:a,u=_e(d,l/c)):(u=o==="auto"?l:o,d=a===null?_e(u,c/l):a==="auto"?c:a);const p={},y=(C,w)=>{Vs(w)||(p[C]=w.toString())};y("width",u),y("height",d);const $=[i.left,i.top,l,c];return p.viewBox=$.join(" "),{attributes:p,viewBox:$,body:r}}function Ne(t,e){let s=t.indexOf("xlink:")===-1?"":' xmlns:xlink="http://www.w3.org/1999/xlink"';for(const n in e)s+=" "+n+'="'+e[n]+'"';return'<svg xmlns="http://www.w3.org/2000/svg"'+s+">"+t+"</svg>"}function Ws(t){return t.replace(/"/g,"'").replace(/%/g,"%25").replace(/#/g,"%23").replace(/</g,"%3C").replace(/>/g,"%3E").replace(/\s+/g," ")}function Ks(t){return"data:image/svg+xml,"+Ws(t)}function Mt(t){return'url("'+Ks(t)+'")'}const Qs=()=>{let t;try{if(t=fetch,typeof t=="function")return t}catch{}};let ce=Qs();function Gs(t){ce=t}function Ys(){return ce}function Zs(t,e){const s=pe(t);if(!s)return 0;let n;if(!s.maxURL)n=0;else{let i=0;s.resources.forEach(o=>{i=Math.max(i,o.length)});const r=e+".json?icons=";n=s.maxURL-i-s.path.length-r.length}return n}function Xs(t){return t===404}const ei=(t,e,s)=>{const n=[],i=Zs(t,e),r="icons";let o={type:r,provider:t,prefix:e,icons:[]},a=0;return s.forEach((l,c)=>{a+=l.length+1,a>=i&&c>0&&(n.push(o),o={type:r,provider:t,prefix:e,icons:[]},a=l.length),o.icons.push(l)}),n.push(o),n};function ti(t){if(typeof t=="string"){const e=pe(t);if(e)return e.path}return"/"}const si=(t,e,s)=>{if(!ce){s("abort",424);return}let n=ti(e.provider);switch(e.type){case"icons":{const r=e.prefix,o=e.icons.join(","),a=new URLSearchParams({icons:o});n+=r+".json?"+a.toString();break}case"custom":{const r=e.uri;n+=r.slice(0,1)==="/"?r.slice(1):r;break}default:s("abort",400);return}let i=503;ce(t+n).then(r=>{const o=r.status;if(o!==200){setTimeout(()=>{s(Xs(o)?"abort":"next",o)});return}return i=501,r.json()}).then(r=>{if(typeof r!="object"||r===null){setTimeout(()=>{r===404?s("abort",r):s("next",i)});return}setTimeout(()=>{s("success",r)})}).catch(()=>{s("next",i)})},ii={prepare:ei,send:si};function ni(t,e,s){A(s||"",e).loadIcons=t}function ri(t,e,s){A(s||"",e).loadIcon=t}const ye="data-style";let Rt="";function oi(t){Rt=t}function it(t,e){let s=Array.from(t.childNodes).find(n=>n.hasAttribute&&n.hasAttribute(ye));s||(s=document.createElement("style"),s.setAttribute(ye,ye),t.appendChild(s)),s.textContent=":host{display:inline-block;vertical-align:"+(e?"-0.125em":"0")+"}span,svg{display:block;margin:auto}"+Rt}function Lt(){Ye("",ii),Et(!0);let t;try{t=window}catch{}if(t){if(t.IconifyPreload!==void 0){const s=t.IconifyPreload,n="Invalid IconifyPreload syntax.";typeof s=="object"&&s!==null&&(s instanceof Array?s:[s]).forEach(i=>{try{(typeof i!="object"||i===null||i instanceof Array||typeof i.icons!="object"||typeof i.prefix!="string"||!Ge(i))&&console.error(n)}catch{console.error(n)}})}if(t.IconifyProviders!==void 0){const s=t.IconifyProviders;if(typeof s=="object"&&s!==null)for(const n in s){const i="IconifyProviders["+n+"] is invalid.";try{const r=s[n];if(typeof r!="object"||!r||r.resources===void 0)continue;Ze(n,r)||console.error(i)}catch{console.error(i)}}}}return{iconLoaded:ks,getIcon:Ss,listIcons:_s,addIcon:Pt,addCollection:Ge,calculateSize:_e,buildIcon:Nt,iconToHTML:Ne,svgToURL:Mt,loadIcons:De,loadIcon:Ls,addAPIProvider:Ze,setCustomIconLoader:ri,setCustomIconsLoader:ni,appendCustomStyle:oi,_api:{getAPIConfig:pe,setAPIModule:Ye,sendAPIQuery:jt,setFetch:Gs,getFetch:Ys,listAPIProviders:Is}}}const ke={"background-color":"currentColor"},Ut={"background-color":"transparent"},nt={image:"var(--svg)",repeat:"no-repeat",size:"100% 100%"},rt={"-webkit-mask":ke,mask:ke,background:Ut};for(const t in rt){const e=rt[t];for(const s in nt)e[t+"-"+s]=nt[s]}function ot(t){return t?t+(t.match(/^[-0-9.]+$/)?"px":""):"inherit"}function ai(t,e,s){const n=document.createElement("span");let i=t.body;i.indexOf("<a")!==-1&&(i+="<!-- "+Date.now()+" -->");const r=t.attributes,o=Ne(i,{...r,width:e.width+"",height:e.height+""}),a=Mt(o),l=n.style,c={"--svg":a,width:ot(r.width),height:ot(r.height),...s?ke:Ut};for(const u in c)l.setProperty(u,c[u]);return n}let B;function li(){try{B=window.trustedTypes.createPolicy("iconify",{createHTML:t=>t})}catch{B=null}}function ci(t){return B===void 0&&li(),B?B.createHTML(t):t}function di(t){const e=document.createElement("span"),s=t.attributes;let n="";s.width||(n="width: inherit;"),s.height||(n+="height: inherit;"),n&&(s.style=n);const i=Ne(t.body,s);return e.innerHTML=ci(i),e.firstChild}function Se(t){return Array.from(t.childNodes).find(e=>{const s=e.tagName&&e.tagName.toUpperCase();return s==="SPAN"||s==="SVG"})}function at(t,e){const s=e.icon.data,n=e.customisations,i=Nt(s,n);n.preserveAspectRatio&&(i.attributes.preserveAspectRatio=n.preserveAspectRatio);const r=e.renderedMode;let o;r==="svg"?o=di(i):o=ai(i,{...Z,...s},r==="mask");const a=Se(t);a?o.tagName==="SPAN"&&a.tagName===o.tagName?a.setAttribute("style",o.getAttribute("style")):t.replaceChild(o,a):t.appendChild(o)}function lt(t,e,s){const n=s&&(s.rendered?s:s.lastRender);return{rendered:!1,inline:e,icon:t,lastRender:n}}function ui(t="iconify-icon"){let e,s;try{e=window.customElements,s=window.HTMLElement}catch{return}if(!e||!s)return;const n=e.get(t);if(n)return n;const i=["icon","mode","inline","noobserver","width","height","rotate","flip"],r=class extends s{_shadowRoot;_initialised=!1;_state;_checkQueued=!1;_connected=!1;_observer=null;_visible=!0;constructor(){super();const a=this._shadowRoot=this.attachShadow({mode:"open"}),l=this.hasAttribute("inline");it(a,l),this._state=lt({value:""},l),this._queueCheck()}connectedCallback(){this._connected=!0,this.startObserver()}disconnectedCallback(){this._connected=!1,this.stopObserver()}static get observedAttributes(){return i.slice(0)}attributeChangedCallback(a){switch(a){case"inline":{const l=this.hasAttribute("inline"),c=this._state;l!==c.inline&&(c.inline=l,it(this._shadowRoot,l));break}case"noobserver":{this.hasAttribute("noobserver")?this.startObserver():this.stopObserver();break}default:this._queueCheck()}}get icon(){const a=this.getAttribute("icon");if(a&&a.slice(0,1)==="{")try{return JSON.parse(a)}catch{}return a}set icon(a){typeof a=="object"&&(a=JSON.stringify(a)),this.setAttribute("icon",a)}get inline(){return this.hasAttribute("inline")}set inline(a){a?this.setAttribute("inline","true"):this.removeAttribute("inline")}get observer(){return this.hasAttribute("observer")}set observer(a){a?this.setAttribute("observer","true"):this.removeAttribute("observer")}restartAnimation(){const a=this._state;if(a.rendered){const l=this._shadowRoot;if(a.renderedMode==="svg")try{l.lastChild.setCurrentTime(0);return}catch{}at(l,a)}}get status(){const a=this._state;return a.rendered?"rendered":a.icon.data===null?"failed":"loading"}_queueCheck(){this._checkQueued||(this._checkQueued=!0,setTimeout(()=>{this._check()}))}_check(){if(!this._checkQueued)return;this._checkQueued=!1;const a=this._state,l=this.getAttribute("icon");if(l!==a.icon.value){this._iconChanged(l);return}if(!a.rendered||!this._visible)return;const c=this.getAttribute("mode"),u=Ke(this);(a.attrMode!==c||ms(a.customisations,u)||!Se(this._shadowRoot))&&this._renderIcon(a.icon,u,c)}_iconChanged(a){const l=Us(a,(c,u,d)=>{const p=this._state;if(p.rendered||this.getAttribute("icon")!==c)return;const y={value:c,name:u,data:d};y.data?this._gotIconData(y):p.icon=y});l.data?this._gotIconData(l):this._state=lt(l,this._state.inline,this._state)}_forceRender(){if(!this._visible){const a=Se(this._shadowRoot);a&&this._shadowRoot.removeChild(a);return}this._queueCheck()}_gotIconData(a){this._checkQueued=!1,this._renderIcon(a,Ke(this),this.getAttribute("mode"))}_renderIcon(a,l,c){const u=Fs(a.data.body,c),d=this._state.inline;at(this._shadowRoot,this._state={rendered:!0,icon:a,inline:d,customisations:l,attrMode:c,renderedMode:u})}startObserver(){if(!this._observer&&!this.hasAttribute("noobserver"))try{this._observer=new IntersectionObserver(a=>{const l=a.some(c=>c.isIntersecting);l!==this._visible&&(this._visible=l,this._forceRender())}),this._observer.observe(this)}catch{if(this._observer){try{this._observer.disconnect()}catch{}this._observer=null}}}stopObserver(){this._observer&&(this._observer.disconnect(),this._observer=null,this._visible=!0,this._connected&&this._forceRender())}};i.forEach(a=>{a in r.prototype||Object.defineProperty(r.prototype,a,{get:function(){return this.getAttribute(a)},set:function(l){l!==null?this.setAttribute(a,l):this.removeAttribute(a)}})});const o=Lt();for(const a in o)r[a]=r.prototype[a]=o[a];return e.define(t,r),r}const hi=ui()||Lt(),{iconLoaded:Ai,getIcon:Ti,listIcons:Ei,addIcon:Pi,addCollection:Ii,calculateSize:Oi,buildIcon:ji,iconToHTML:Di,svgToURL:Ni,loadIcons:Mi,loadIcon:Ri,setCustomIconLoader:Li,setCustomIconsLoader:Ui,addAPIProvider:Fi,_api:qi}=hi;async function x(t,e){const s=await fetch(t,{...e,headers:{...e?.body?{"content-type":"application/json"}:{},...e?.headers}});if(!s.ok){const n=await s.json().catch(()=>({error:s.statusText}));throw new Error(n.error||s.statusText)}return s.status===204?void 0:s.json()}function Ft(t,e){return e==="telegram"?{type:"telegram",name:t.get("name"),bot_token:t.get("bot_token"),chat_id:t.get("chat_id")}:{type:"webhook",name:t.get("name"),url:t.get("url"),headers:{}}}function qt(t,e=[]){return{name:String(t.get("name")),url:String(t.get("url")),method:String(t.get("method")),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),headers:{},body:null,body_contains:null,skip_tls_verification:!1,notification_channel_ids:e}}var pi=Object.defineProperty,fi=Object.getOwnPropertyDescriptor,U=(t,e,s,n)=>{for(var i=n>1?void 0:n?fi(e,s):e,r=t.length-1,o;r>=0;r--)(o=t[r])&&(i=(n?o(e,s,i):o(i))||i);return n&&i&&pi(e,s,i),i};let E=class extends M{constructor(){super(...arguments),this.channelKind="webhook",this.channels=[],this.saving=!1,this.error=""}connectedCallback(){super.connectedCallback(),this.loadChannels()}updated(t){t.has("setup")&&this.loadChannels()}async loadChannels(){if(!(!this.setup?.cluster_ready||this.setup.phase!=="target"))try{this.channels=await x("/api/v1/channels")}catch(t){this.fail(t)}}submittedNodeName(){return this.shadowRoot?.querySelector("#setup-node-name")?.value.trim()??""}async createCluster(t){t.preventDefault(),window.confirm("Create a new single-Node Cluster?")&&await this.choose("/api/v1/setup/new-cluster",{node_name:this.submittedNodeName()})}async joinCluster(t){t.preventDefault();const e=t.currentTarget,s=new FormData(e);await this.choose("/api/v1/cluster/join",{node_name:this.submittedNodeName(),join_link:String(s.get("join_link")??"").trim()})}async choose(t,e){this.saving=!0,this.error="";try{await x(t,{method:"POST",body:JSON.stringify(e)}),await this.waitForCluster()}catch(s){this.fail(s),this.saving=!1}}async waitForCluster(){for(let t=0;t<120;t+=1){await new Promise(e=>window.setTimeout(e,250));try{const e=await x("/api/v1/setup");if(e.cluster_ready){this.changed(e);return}}catch{}}throw new Error("Cluster setup did not finish within 30 seconds")}async createChannel(t){t.preventDefault();const e=new FormData(t.currentTarget),s=Ft(e,this.channelKind);await this.createResource("/api/v1/channels",s)}async createTarget(t){t.preventDefault();const e=new FormData(t.currentTarget),s=qt(e,e.getAll("channel_id").map(String));await this.createResource("/api/v1/targets",s)}async createResource(t,e){this.saving=!0;try{await x(t,{method:"POST",body:JSON.stringify(e)}),await this.next()}catch(s){this.fail(s),this.saving=!1}}async next(){this.saving=!0;try{this.changed(await x("/api/v1/setup/next",{method:"POST"}))}catch(t){this.fail(t),this.saving=!1}}changed(t){this.saving=!1,this.dispatchEvent(new CustomEvent("setup-changed",{detail:t,bubbles:!0,composed:!0}))}fail(t){this.error=t instanceof Error?t.message:String(t)}render(){return h`<section class="flow" aria-label="UpGrid setup">
      ${this.error?h`<div class="notice" role="alert">${this.error}</div>`:f}
      ${this.setup.phase==="cluster"?this.renderCluster():this.setup.phase==="channel"?this.renderChannel():this.renderTarget()}
    </section>`}renderCluster(){return h`
      <span class="eyebrow">First-run setup</span><h1>Choose your Cluster</h1>
      <p class="lead">Review this Node’s name, then create a new Cluster or use an invitation to join one.</p>
      <div class="cluster-panel">
        <div class="cluster-identity">
          <label for="setup-node-name">Node name<input id="setup-node-name" .value=${this.setup.node_name} required /></label>
        </div>
        <form class="cluster-create" @submit=${this.createCluster}>
          <div class="cluster-copy"><h2>Start a new Cluster</h2><p>This Node becomes the first voting member.</p></div>
          <button type="submit" ?disabled=${this.saving}>${this.saving?"Setting up…":"Create new Cluster"}</button>
        </form>
        <div class="cluster-divider"><span>Or</span></div>
        <form class="cluster-join" @submit=${this.joinCluster}>
          <div class="cluster-copy"><h2>Join an existing Cluster</h2><p>Paste an <code>up://</code> Join Token from a current member.</p></div>
          <div class="cluster-join-fields">
            <label>Join Token<input name="join_link" type="url" pattern="up://.*" placeholder="up://node.example/token" autocomplete="off" required /></label>
            <button class="secondary" type="submit" ?disabled=${this.saving}>Join Cluster</button>
          </div>
        </form>
      </div>`}renderChannel(){return h`
      <span class="eyebrow">Optional · Step 2 of 3</span><h1>Add a notification channel</h1>
      <p class="lead">Send availability transitions to Telegram or a webhook. <span class="count">${this.setup.channel_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createChannel}>
        <label>Type<select name="type" @change=${t=>this.channelKind=t.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
        <label>Name<input name="name" placeholder="On-call" required /></label>
        ${this.channelKind==="webhook"?h`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:h`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and continue</button></div>
      </form></div>`}renderTarget(){return h`
      <span class="eyebrow">Optional · Step 3 of 3</span><h1>Monitor your first Target</h1>
      <p class="lead">Configure an HTTP endpoint now or continue to the dashboard. <span class="count">${this.setup.target_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createTarget}>
        <label>Name<input name="name" placeholder="Production API" required /></label>
        <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
        <div class="row"><label>Method<input name="method" value="GET" required /></label><label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label></div>
        <div class="row"><label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label><label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label></div>
        ${this.channels.length?h`<fieldset><legend>Notification channels</legend>${this.channels.map(t=>h`<label><span><input name="channel_id" type="checkbox" value=${t.id} /> ${t.name}</span></label>`)}</fieldset>`:f}
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and finish</button></div>
      </form></div>`}};E.styles=ht`
    :host { display: block; }
    *, *::before, *::after { box-sizing: border-box; }
    .flow { width: min(760px, 100%); margin: 0 auto; }
    .eyebrow { color: var(--muted); font-size: 12px; letter-spacing: .16em; text-transform: uppercase; }
    h1 { margin: 5px 0 8px; font-size: clamp(30px, 5vw, 46px); letter-spacing: -.04em; }
    .lead { margin: 0 0 16px; color: var(--muted); font-size: 15px; }
    .panel { border: 1px solid var(--line); border-radius: 16px; background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); overflow: hidden; }
    .choice { display: grid; gap: 14px; padding: 22px; border-top: 1px solid var(--line); }
    .choice:first-child { border-top: 0; }
    .choice h2 { margin: 0; font-size: 17px; }
    .choice p { margin: -8px 0 0; color: var(--muted); }
    .cluster-panel { border: 1px solid var(--line); border-radius: 16px; background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); overflow: hidden; }
    .cluster-identity, .cluster-create, .cluster-join { padding: 15px 18px; }
    .cluster-identity { border-bottom: 1px solid var(--line); }
    .cluster-create { display: flex; align-items: center; justify-content: space-between; gap: 18px; }
    .cluster-copy h2 { margin: 0; font-size: 17px; }
    .cluster-copy p { margin: 2px 0 0; color: var(--muted); }
    .cluster-divider { display: flex; align-items: center; gap: 12px; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .12em; }
    .cluster-divider::before, .cluster-divider::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    .cluster-join { display: grid; gap: 10px; }
    .cluster-join-fields { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: end; gap: 10px; }
    .cluster-join-fields label { min-width: 0; }
    .cluster-join-fields button { height: 41px; white-space: nowrap; }
    form { display: grid; gap: 13px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; }
    input, select { width: 100%; min-height: 41px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; transition: border-color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .actions { display: flex; justify-content: flex-end; gap: 9px; margin-top: 5px; }
    button { display: inline-flex; min-height: 41px; align-items: center; justify-content: center; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    button:hover { border-color: var(--button-hover-border); }
    button:active { transform: translateY(1px); }
    button:disabled { cursor: not-allowed; opacity: .65; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .notice { margin-bottom: 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .count { display: inline-block; margin-top: 6px; color: var(--green); font-size: 12px; }
    @media (max-width: 620px) { .row, .cluster-join-fields { grid-template-columns: 1fr; } .cluster-create { align-items: stretch; flex-direction: column; } .cluster-create button, .cluster-join button { justify-self: end; } }
  `;U([vt({attribute:!1})],E.prototype,"setup",2);U([g()],E.prototype,"channelKind",2);U([g()],E.prototype,"channels",2);U([g()],E.prototype,"saving",2);U([g()],E.prototype,"error",2);E=U([bt("upgrid-setup")],E);const gi={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3a6 6 0 0 0 9 9a9 9 0 1 1-9-9Z"/>'},mi={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="13.5" cy="6.5" r=".5"/><circle cx="17.5" cy="10.5" r=".5"/><circle cx="8.5" cy="7.5" r=".5"/><circle cx="6.5" cy="12.5" r=".5"/><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c.926 0 1.648-.746 1.648-1.688c0-.437-.18-.835-.437-1.125c-.29-.289-.438-.652-.438-1.125a1.64 1.64 0 0 1 1.668-1.668h1.996c3.051 0 5.555-2.503 5.555-5.554C21.965 6.012 17.461 2 12 2z"/></g>'},bi={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></g>'};var vi=Object.defineProperty,v=(t,e,s,n)=>{for(var i=void 0,r=t.length-1,o;r>=0;r--)(o=t[r])&&(i=o(e,s,i)||i);return i&&vi(e,s,i),i};const ne=["system","dark","bright"],ct={system:mi,dark:gi,bright:bi},Me={overview:"/",alerts:"/alerts",cluster:"/cluster"};function dt(){return Object.entries(Me).find(([,t])=>t===window.location.pathname)?.[0]??"overview"}function yi(){const t=localStorage.getItem("upgrid-theme");return ne.includes(t)?t:"system"}class b extends M{constructor(){super(...arguments),this.targets=[],this.channels=[],this.alerts=[],this.secrets=[],this.joinTokens=[],this.error="",this.live=!1,this.saving=!1,this.channelKind="webhook",this.joinCommand="",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection=dt(),this.copied=!1,this.setupMode=!1,this.warningDismissed=sessionStorage.getItem("upgrid-warning-dismissed")==="1",this.unlimitedUses=!1,this.theme=yi(),this.detailDirty=!1,this.detailInitialState="",this.systemTheme=matchMedia("(prefers-color-scheme: light)"),this.systemThemeChanged=()=>{this.theme==="system"&&this.applyTheme()},this.routeChanged=()=>{if(this.setupMode&&this.setup){window.history.replaceState(null,"",this.setup.path);return}this.activeSection=dt()}}connectedCallback(){super.connectedCallback(),this.applyTheme(),this.systemTheme.addEventListener("change",this.systemThemeChanged),window.addEventListener("popstate",this.routeChanged),this.start()}disconnectedCallback(){this.systemTheme.removeEventListener("change",this.systemThemeChanged),window.removeEventListener("popstate",this.routeChanged),this.events?.close(),super.disconnectedCallback()}async start(){try{const e=await x("/api/v1/setup");if(this.setup=e,this.setupMode=e.setup,this.setupMode){window.history.replaceState(null,"",e.path),e.cluster_ready?(await this.refresh(),this.connectEvents()):this.live=!0;return}await this.refresh(),this.connectEvents()}catch(e){this.error=e instanceof Error?e.message:String(e)}}connectEvents(){this.events?.close(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}applyTheme(){const e=this.theme==="system"?this.systemTheme.matches?"bright":"dark":this.theme;this.dataset.theme=e,document.querySelector('meta[name="theme-color"]')?.setAttribute("content",e==="bright"?"#f4f8f6":"#0b1110")}cycleTheme(){this.theme=ne[(ne.indexOf(this.theme)+1)%ne.length],localStorage.setItem("upgrid-theme",this.theme),this.applyTheme()}dismissWarning(){sessionStorage.setItem("upgrid-warning-dismissed","1"),this.warningDismissed=!0}async refresh(){try{[this.targets,this.channels,this.alerts,this.secrets,this.cluster,this.joinTokens]=await Promise.all([x("/api/v1/targets"),x("/api/v1/channels"),x("/api/v1/alerts"),x("/api/v1/secrets"),x("/api/v1/cluster"),x("/api/v1/join-tokens")]),this.error=""}catch(e){this.error=e instanceof Error?e.message:String(e)}}openTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(e){this.detailDirty=!1,this.selected=e,this.updateComplete.then(()=>{const s=this.renderRoot.querySelector("#detail-dialog"),n=s?.querySelector("form");n&&(this.detailInitialState=this.detailFormState(n)),s?.showModal()})}closeDetailDialog(){this.renderRoot.querySelector("#detail-dialog")?.close(),this.detailDirty=!1,this.detailInitialState="",this.selected=void 0}showDialog(e){this.renderRoot.querySelector(`#${e}`)?.showModal()}dismissOnBackdrop(e){const s=e.currentTarget;e.target===s&&(s.close(),s.id==="detail-dialog"&&this.closeDetailDialog())}navigate(e,s){e.preventDefault(),this.activeSection=s,window.history.pushState(null,"",Me[s]),this.updateComplete.then(()=>this.renderRoot.querySelector(`#${s}`)?.scrollIntoView({behavior:"smooth",block:"start"}))}closeDialog(e){this.renderRoot.querySelector(`#${e}`)?.close()}toggleMaxRedirects(e){const s=e.currentTarget,n=s.form?.elements.namedItem("max_redirects");n&&(n.disabled=!s.checked),s.form&&this.compareDetailForm(s.form)}detailFormState(e){return JSON.stringify([...new FormData(e).entries()])}compareDetailForm(e){this.detailDirty=this.detailFormState(e)!==this.detailInitialState}updateDetailDirty(e){this.compareDetailForm(e.currentTarget)}}v([g()],b.prototype,"targets");v([g()],b.prototype,"channels");v([g()],b.prototype,"alerts");v([g()],b.prototype,"secrets");v([g()],b.prototype,"cluster");v([g()],b.prototype,"joinTokens");v([g()],b.prototype,"error");v([g()],b.prototype,"live");v([g()],b.prototype,"saving");v([g()],b.prototype,"selected");v([g()],b.prototype,"channelKind");v([g()],b.prototype,"joinCommand");v([g()],b.prototype,"search");v([g()],b.prototype,"statusFilter");v([g()],b.prototype,"sort");v([g()],b.prototype,"selectedIds");v([g()],b.prototype,"activeSection");v([g()],b.prototype,"copied");v([g()],b.prototype,"setupMode");v([g()],b.prototype,"setup");v([g()],b.prototype,"warningDismissed");v([g()],b.prototype,"unlimitedUses");v([g()],b.prototype,"theme");v([g()],b.prototype,"detailDirty");class xi extends b{async createTarget(e){e.preventDefault();const s=e.currentTarget,n=new FormData(s),i=qt(n);this.saving=!0;try{await x("/api/v1/targets",{method:"POST",body:JSON.stringify(i)}),s.reset(),this.closeTargetDialog(),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async updateTarget(e){if(e.preventDefault(),!this.selected)return;const s=new FormData(e.currentTarget),n=s.get("follow_redirects")==="on",i={name:String(s.get("name")),url:String(s.get("url")),method:String(s.get("method")),accepted_statuses:String(s.get("statuses")).split(",").map(r=>{const[o,a]=r.trim().split("-").map(Number);return{start:o,end:a||o}}),follow_redirects:n,max_redirects:n?Number(s.get("max_redirects")):0,interval_seconds:Number(s.get("interval")),timeout_seconds:Number(s.get("timeout")),failure_threshold:Number(s.get("failures")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([r,o])=>[r,o.kind==="literal"?o.value:{secret_id:o.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,body_contains:String(s.get("body_contains"))||null,skip_tls_verification:s.get("skip_tls_verification")==="on",notification_channel_ids:this.selected.notification_channel_ids};this.saving=!0;try{await x(`/api/v1/targets/${this.selected.id}`,{method:"PUT",body:JSON.stringify(i)}),this.closeDetailDialog(),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Delete this target and its history?"))){this.saving=!0;try{await x(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async setPaused(e){if(this.selected){this.saving=!0;try{await x(`/api/v1/targets/${this.selected.id}/${e?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}}async createSecret(e){e.preventDefault();const s=e.currentTarget,n=new FormData(s);this.saving=!0;try{await x("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:n.get("name"),value:n.get("value")})}),s.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async createChannel(e){e.preventDefault();const s=e.currentTarget,n=new FormData(s),i=Ft(n,this.channelKind);this.saving=!0;try{await x("/api/v1/channels",{method:"POST",body:JSON.stringify(i)}),s.reset(),this.channelKind="webhook",this.closeDialog("channel-dialog"),await this.refresh()}catch(r){this.error=r instanceof Error?r.message:String(r)}finally{this.saving=!1}}openTokenDialog(){this.unlimitedUses=!1,this.showDialog("token-config-dialog")}async createJoinToken(e){e.preventDefault();const s=new FormData(e.currentTarget);this.saving=!0;try{const n=await x("/api/v1/join-tokens",{method:"POST",body:JSON.stringify({expires_in_seconds:Number(s.get("expiration_days"))*86400,max_uses:this.unlimitedUses?null:Number(s.get("max_uses"))})});this.joinCommand=`upgrid --join '${n.url}'`,this.copied=!1,await this.refresh(),this.closeDialog("token-config-dialog"),this.showDialog("join-dialog")}catch(n){this.error=n instanceof Error?n.message:String(n)}finally{this.saving=!1}}async setupChanged(e){const s=e.detail;if(this.setup=s,this.setupMode=s.setup,window.history.replaceState(null,"",s.path),s.setup){s.cluster_ready&&(await this.refresh(),this.connectEvents());return}this.activeSection="overview",await this.refresh(),this.connectEvents()}async revokeJoinToken(e){if(window.confirm("Revoke this Join Token? Nodes using it will no longer be admitted.")){this.saving=!0;try{await x(`/api/v1/join-tokens/${e.id}`,{method:"DELETE"}),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}}async copyJoinCommand(){let e=!1;try{await navigator.clipboard.writeText(this.joinCommand),e=!0}catch{const s=Object.assign(document.createElement("textarea"),{value:this.joinCommand});s.style.cssText="position: fixed; opacity: 0",document.body.append(s),s.select(),e=document.execCommand("copy"),s.remove()}if(!e){this.error="Could not copy the Join command";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(e,s){const n=new Set(this.selectedIds);s?n.add(e):n.delete(e),this.selectedIds=n}async bulkPause(e){this.saving=!0;try{await Promise.all([...this.selectedIds].map(s=>x(`/api/v1/targets/${s}/${e?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Delete ${this.selectedIds.size} selected Targets and their history?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>x(`/api/v1/targets/${e}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async deleteResource(e,s,n){if(window.confirm(`Delete ${n}?`))try{await x(`/api/v1/${e}/${s}`,{method:"DELETE"}),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}}}const $i={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2m-6 5v6m4-6v6"/>'};function wi(t,e,s,n,i){const r=t.accepted_statuses.map(d=>d.start===d.end?d.start:`${d.start}-${d.end}`).join(","),o=t.history.slice(0,30).reverse(),a=Math.max(1,...o.map(d=>d.latency_ms)),l=new Map(n.map(d=>[d.id,d.name])),c=d=>new Date(d).toLocaleString(void 0,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}),u=d=>d>=1e3?`${(d/1e3).toFixed(d>=1e4?0:1)} s`:`${Math.round(d)} ms`;return h`
    <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${i.backdrop}>
      <div class="dialog-head">
        <h2 id="target-detail-title">Target details</h2>
        <button class="button secondary icon-button dialog-close" type="button" aria-label="Close target details" title="Close" @click=${i.close}><iconify-icon .icon=${$t} aria-hidden="true"></iconify-icon></button>
      </div>
      <form @submit=${i.update} @input=${i.changed}>
        <label>Name<input name="name" .value=${t.name} required /></label>
        <label>URL<input name="url" type="url" .value=${t.url} required /></label>
        <div class="row"><label>Method<input name="method" .value=${t.method} required /></label><label>Expected statuses<input name="statuses" .value=${r} required /></label></div>
        <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(t.interval_seconds)} required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(t.timeout_seconds)} required /></label></div>
        <div class="row"><label>Failures before Down<input name="failures" type="number" min="1" .value=${String(t.failure_threshold)} required /></label><label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(t.max_redirects)} ?disabled=${!t.follow_redirects} required /></label></div>
        <label>Body must contain<input name="body_contains" .value=${t.body_contains??""} /></label>
        <div class="row"><label class="check"><input name="follow_redirects" type="checkbox" .checked=${t.follow_redirects} @change=${i.redirects} />Follow redirects</label><label class="check"><input name="skip_tls_verification" type="checkbox" .checked=${t.skip_tls_verification} />Skip TLS verification</label></div>
        <div class="dialog-actions">
          <div class="danger-actions">
            <button class="button danger icon-button" type="button" aria-label="Delete target" title="Delete target" @click=${i.delete}><iconify-icon .icon=${$i} aria-hidden="true"></iconify-icon></button>
            <button class=${`button ${t.paused?"success":"warning"} icon-button`} type="button" aria-label=${t.paused?"Resume evaluations":"Pause evaluations"} title=${t.paused?"Resume evaluations":"Pause evaluations"} @click=${()=>i.pause(!t.paused)}><iconify-icon .icon=${t.paused?xt:yt} aria-hidden="true"></iconify-icon></button>
          </div>
          <button class="button" type="submit" aria-busy=${e?"true":"false"} ?disabled=${e||!s}>Save changes</button>
        </div>
      </form>
      <section class="history">
        <div class="history-head"><h3>Evaluation history</h3>${o.length?h`<span class="meta">Latest ${o.length}</span>`:f}</div>
        ${o.length?h`
          <div class="chart-plot">
            <div class="chart-scale" aria-hidden="true"><span>${u(a)}</span><span>${u(a/2)}</span><span>0 ms</span></div>
            <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${u(a)}`}>
              ${o.map(d=>{const p=d.succeeded?"Passed":"Failed",y=d.status_code===null?"network error":`HTTP ${d.status_code}`,$=l.get(d.executor_node_id)??`Node ${d.executor_node_id.slice(0,8)}`,C=`${p} at ${new Date(d.recorded_at_ms).toLocaleString()}: ${d.latency_ms} ms, ${y}. Executed by ${$}`;return h`<span class="history-bar ${d.succeeded?"up":"down"}" role="listitem" aria-label=${C} title=${C} style=${`height: ${Math.max(8,d.latency_ms/a*100)}%`}></span>`})}
            </div>
          </div>
          <div class="chart-axis"><span>${c(o[0].recorded_at_ms)}</span><span>${c(o.at(-1).recorded_at_ms)}</span></div>
          <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
        `:h`<p class="meta">No evaluations recorded yet.</p>`}
      </section>
    </dialog>`}var _i=Object.getOwnPropertyDescriptor,ki=(t,e,s,n)=>{for(var i=n>1?void 0:n?_i(e,s):e,r=t.length-1,o;r>=0;r--)(o=t[r])&&(i=o(i)||i);return i};let Ce=class extends xi{render(){const t=this.targets.filter(r=>r.availability==="up").length,e=this.targets.filter(r=>r.availability==="down").length,s=this.alerts.filter(r=>r.delivery==="pending").length,n=["overview","alerts","cluster"],i=this.targets.filter(r=>`${r.name} ${r.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(r=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?r.paused:r.availability===this.statusFilter).sort((r,o)=>this.sort==="status"&&r.availability.localeCompare(o.availability)||r.name.localeCompare(o.name));return this.setupMode&&this.setup?h`
        <main class="shell setup-shell">
          <header>
            <div class="brand">
              <img src="/favicon.svg" alt="" />
              <div><div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${this.live?"on":""}"></i>${this.live?"ready":"connecting"}</div></div><span>Distributed service monitoring</span></div>
            </div>
            <div></div>
            <div class="actions"><button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${ct[this.theme]} aria-hidden="true"></iconify-icon></button></div>
          </header>
          ${this.error?h`<div class="notice" role="alert">${this.error}</div>`:f}
          <upgrid-setup .setup=${this.setup} @setup-changed=${this.setupChanged}></upgrid-setup>
        </main>`:h`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div>
              <div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${this.live?"on":""}"></i>${this.live?"live":"connecting"}</div></div>
              <span>Distributed service monitoring</span>
            </div>
          </div>
          <nav aria-label="Primary">
            ${n.map(r=>h`<a class=${this.activeSection===r?"active":""} href=${Me[r]} @click=${o=>this.navigate(o,r)}>${r[0].toUpperCase()}${r.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${ct[this.theme]} aria-hidden="true"></iconify-icon></button>
          </div>
        </header>
        ${this.error?h`<div class="notice" role="alert">${this.error}</div>`:f}
        ${this.setup?.warning&&!this.warningDismissed?h`<div class="notice" role="status">${this.setup.warning}<button class="button secondary" style="float: right; margin: -6px" @click=${this.dismissWarning}>Dismiss</button></div>`:f}
        ${this.activeSection==="overview"?this.renderOverview(i,t,e,s):this.activeSection==="alerts"?this.renderAlertsPage():this.renderClusterPage()}
      </main>
      <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="add-target-title">Add target</h2><p>Start monitoring an HTTP or HTTPS endpoint.</p></div>
        <form @submit=${this.createTarget}>
          <label>Name<input name="name" placeholder="Production API" required /></label>
          <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
          <div class="row">
            <label>Method<input name="method" value="GET" required /></label>
            <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
          </div>
          <div class="row">
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
            <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
          </div>
          <div class="dialog-actions">
            <button class="button secondary" type="button" @click=${this.closeTargetDialog}>Cancel</button>
            <button class="button" type="submit" ?disabled=${this.saving}>${this.saving?"Creating…":"Create target"}</button>
          </div>
        </form>
      </dialog>
      ${this.selected?wi(this.selected,this.saving,this.detailDirty,this.cluster?.members??[],{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeDetailDialog(),update:r=>{this.updateTarget(r)},changed:r=>this.updateDetailDirty(r),redirects:r=>this.toggleMaxRedirects(r),delete:()=>{this.deleteTarget()},pause:r=>{this.setPaused(r)}}):f}
      <dialog id="secret-dialog" aria-labelledby="secret-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="secret-title">Add secret</h2><p>The plaintext is encrypted before replication and never returned.</p></div>
        <form @submit=${this.createSecret}>
          <label>Name<input name="name" placeholder="Webhook token" required /></label>
          <label>Value<input name="value" type="password" autocomplete="new-password" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("secret-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create secret</button></div>
        </form>
      </dialog>
      <dialog id="channel-dialog" aria-labelledby="channel-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="channel-title">Add channel</h2><p>Send transitions through Telegram or a generic webhook.</p></div>
        <form @submit=${this.createChannel}>
          <label>Type<select name="type" @change=${r=>this.channelKind=r.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
          <label>Name<input name="name" placeholder="On-call" required /></label>
          ${this.channelKind==="webhook"?h`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:h`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("channel-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create channel</button></div>
        </form>
      </dialog>
      <dialog id="token-config-dialog" aria-labelledby="token-config-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="token-config-title">Create Join Token</h2><p>Choose how many days the token remains valid and whether it can be reused.</p></div>
        <form @submit=${this.createJoinToken}>
          <label>Expiration (days)<input name="expiration_days" type="number" min="1" step="1" value="1" required /></label>
          <label class="switch"><span>Unlimited uses</span><input type="checkbox" role="switch" .checked=${this.unlimitedUses} @change=${r=>this.unlimitedUses=r.target.checked} /></label>
          <label>Maximum uses<input name="max_uses" type="number" min="1" step="1" value="1" ?disabled=${this.unlimitedUses} required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("token-config-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>${this.saving?"Creating…":"Create token"}</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-title">Join Token Created</h2><p>This command contains Cluster credentials. Revoke the token when no longer needed.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinCommand}>${this.copied?"Copied":"Copy command"}</button></div>
      </dialog>
    `}renderOverview(t,e,s,n){const i=this.targets.filter(a=>this.selectedIds.has(a.id)),r=i.some(a=>!a.paused),o=i.some(a=>a.paused);return h`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="overview-top">
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
          <div class="metric"><span>Pending alerts</span><strong>${n}</strong></div>
          <div class="metric"><span>Up</span><strong>${e}</strong></div>
          <div class="metric"><span>Down</span><strong>${s}</strong></div>
        </section>
        <section class="panel" aria-label="Secrets">
          <div class="panel-head"><h2>Secrets</h2><button class="button secondary" @click=${()=>this.showDialog("secret-dialog")}>Add secret</button></div>
          ${this.secrets.length?this.secrets.map(a=>h`<div class="resource"><div><strong>${a.name}</strong><code>${a.id}</code></div><div class="actions"><span class="badge">write-only</span><button class="button danger" aria-label=${`Delete secret ${a.name}`} @click=${()=>this.deleteResource("secrets",a.id,a.name)}>Delete</button></div></div>`):h`<div class="empty">No reusable Secrets.</div>`}
        </section>
      </section>
      <section class="panel" aria-label="Targets">
        <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
        <div class="toolbar">
          <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${a=>this.search=a.target.value} />
          <select aria-label="Filter targets" .value=${this.statusFilter} @change=${a=>this.statusFilter=a.target.value}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
          <select aria-label="Sort targets" .value=${this.sort} @change=${a=>this.sort=a.target.value}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
        </div>
        ${this.selectedIds.size?h`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><button class="button secondary icon-button" aria-label="Unselect all" title="Unselect all" @click=${()=>this.selectedIds=new Set}><iconify-icon .icon=${$t} aria-hidden="true"></iconify-icon></button>${r?h`<button class="button warning icon-button" aria-label="Pause selected" title="Pause selected" @click=${()=>this.bulkPause(!0)}><iconify-icon .icon=${yt} aria-hidden="true"></iconify-icon></button>`:f}${o?h`<button class="button success icon-button" aria-label="Resume selected" title="Resume selected" @click=${()=>this.bulkPause(!1)}><iconify-icon .icon=${xt} aria-hidden="true"></iconify-icon></button>`:f}<button class="button danger" @click=${this.bulkDelete}>Delete selected</button></div></div>`:f}
        ${t.length?t.map(a=>this.renderTarget(a)):h`<div class="empty">${this.targets.length?"No Targets match these filters.":"No targets yet. Add the first one to begin monitoring."}</div>`}
      </section>
    `}renderAlertsPage(){return h`
      <section class="heading" id="alerts">
        <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      </section>
      <div class="page-columns">
      <section class="panel" aria-label="Alert history">
        <div class="panel-head"><h2>Availability transitions</h2><span class="meta">${this.alerts.length} events</span></div>
        ${this.alerts.length?this.alerts.map(t=>h`<div class="resource"><div><strong>${t.target_name}</strong><code>${new Date(t.scheduled_at_ms).toLocaleString()}</code></div><span class="badge">${t.kind} · ${t.delivery}</span></div>`):h`<div class="empty">No availability transitions.</div>`}
      </section>
      <section class="panel" aria-label="Notification channels">
        <div class="panel-head"><h2>Notification channels</h2><button class="button secondary" @click=${()=>this.showDialog("channel-dialog")}>Add channel</button></div>
        ${this.channels.length?this.channels.map(t=>h`<div class="resource"><div><strong>${t.name}</strong><code>${t.destination}</code></div><div class="actions"><span class="badge">${t.kind}</span><button class="button danger" aria-label=${`Delete channel ${t.name}`} @click=${()=>this.deleteResource("channels",t.id,t.name)}>Delete</button></div></div>`):h`<div class="empty">No notification channels.</div>`}
      </section>
      </div>
    `}renderClusterPage(){return h`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <div class="actions">
          <button class="button" @click=${this.openTokenDialog}>Create token</button>
        </div>
      </section>
      <div class="page-columns">
      <section class="panel" aria-label="Cluster topology">
        <div class="panel-head"><h2>Nodes</h2><span class="meta">${this.cluster?.members.length??0} members</span></div>
        ${this.cluster?.members.map(t=>h`<div class="resource"><div><strong>${t.name}</strong><code>${t.raft_url}</code></div><div class="actions">${t.local?h`<span class="badge">This node</span>`:f}${t.leader?h`<span class="badge">Leader</span>`:f}</div></div>`)}
        ${this.cluster?.members.length?f:h`<div class="empty">Cluster topology unavailable.</div>`}
      </section>
      <section class="panel" aria-label="Join tokens">
        <div class="panel-head"><h2>Join Tokens</h2><span class="meta">${this.joinTokens.length} stored</span></div>
        ${this.joinTokens.length?this.joinTokens.map(t=>h`
              <div class="resource">
                <div><strong>${t.id.slice(0,12)}…</strong><code>Expires ${new Date(t.expires_at_ms).toLocaleString()} · ${t.remaining_uses===null?"unlimited uses":`${t.remaining_uses} uses left`}</code></div>
                <button class="button danger" aria-label=${`Revoke Join Token ${t.id.slice(0,12)}`} @click=${()=>this.revokeJoinToken(t)}>Revoke</button>
              </div>
            `):h`<div class="empty">No Join Tokens.</div>`}
      </section>
      </div>
    `}renderTarget(t){const e=t.latest_evaluation,s=t.history.slice(0,16).reverse(),n=Math.max(1,...s.map(i=>i.latency_ms));return h`
      <div class="target-wrap">
        <input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} .checked=${this.selectedIds.has(t.id)} @change=${i=>this.toggleSelected(t.id,i.target.checked)} />
        <button class="target" aria-label=${t.name} @click=${()=>this.openTarget(t)}>
          <i class="state ${t.paused?"paused":t.availability}" aria-label=${t.paused?"paused":t.availability}></i>
          <div>
            <h3>${t.name}</h3>
            <div class="meta">${t.paused?"Paused · ":""}${t.method} · ${t.url} · every ${t.interval_seconds}s</div>
          </div>
          <div class="target-side">
            ${s.length?h`<div class="mini-chart" aria-hidden="true">${s.map(i=>h`<i class="mini-bar ${i.succeeded?"up":"down"}" style=${`height: ${Math.max(12,i.latency_ms/n*100)}%`}></i>`)}</div>`:f}
            <div class="latency">
              <strong>${e?`${e.latency_ms} ms`:"—"}</strong>
              <span>${e?e.status_code??"network error":"waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `}};Ce.styles=ht`
    :host {
      color-scheme: dark;
      --bg: #090d0c;
      --panel: #111715;
      --panel-2: #151d1a;
      --line: #27322e;
      --muted: #8fa099;
      --text: #edf7f2;
      --green: #58e29c;
      --red: #ff7575;
      --amber: #f2c264;
      --page-background:
        radial-gradient(circle at 12% -5%, #18392d 0, transparent 30%),
        linear-gradient(145deg, #090d0c 0%, #0c1210 55%, #09100d 100%);
      --brand-shadow: #40d89035;
      --nav-bg: #0d1210aa;
      --active-bg: #202b27;
      --button-border: #3e765a;
      --button-bg: #1c4a35;
      --button-text: #e8fff2;
      --button-hover-border: #62b988;
      --panel-surface: #111715dc;
      --panel-shadow: #0002;
      --divider: #202925;
      --badge-border: #3c554a;
      --badge-text: #a7c3b7;
      --row-hover: #17201c;
      --notice-border: #7b3937;
      --notice-bg: #391b1a;
      --notice-text: #ffb3af;
      --bulk-bg: #16221d;
      --dialog-shadow: #000b;
      --backdrop: #040706cc;
      --input-bg: #0c110f;
      --focus: #4b936c;
      --danger-text: #ff9b97;
      --danger-border: #633b39;
      --warning-bg: #594315;
      --warning-text: #ffd778;
      --warning-border: #9c7625;
      --join-bg: #0b110e;
      display: block;
      min-height: 100vh;
      background: var(--page-background);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
      transition: background 220ms ease, color 180ms ease;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { max-width: 1200px; margin: auto; padding: 28px 24px 72px; }
    .setup-shell { display: grid; min-height: 100vh; grid-template-rows: auto minmax(0, 1fr); padding-top: 20px; padding-bottom: 20px; }
    .setup-shell header { margin-bottom: 18px; } .setup-shell upgrid-setup { align-self: center; }
    header { display: grid; grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr); align-items: center; margin-bottom: 34px; }
    .brand, .actions, .live, nav { display: flex; align-items: center; }
    .brand { gap: 13px; }
    header > .brand { justify-self: start; }
    header > nav { justify-self: center; }
    header > .actions { justify-self: end; }
    .brand-line { display: flex; align-items: center; gap: 12px; }
    .brand img { width: 42px; height: 42px; filter: drop-shadow(0 0 18px var(--brand-shadow)); }
    .brand strong { display: block; font-size: 19px; letter-spacing: .02em; }
    .brand span, .live, .eyebrow, .meta { color: var(--muted); font-size: 12px; }
    nav { gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 11px; background: var(--nav-bg); }
    nav a { color: var(--muted); padding: 7px 11px; text-decoration: none; border-radius: 7px; transition: background-color 160ms ease, color 160ms ease; }
    nav a.active { color: var(--text); background: var(--active-bg); }
    .actions { gap: 12px; }
    .live { gap: 7px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--amber); transition: background-color 160ms ease, box-shadow 160ms ease; }
    .dot.on { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 18px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .button:disabled { cursor: not-allowed; opacity: .65; }
    .button[aria-busy="true"] { cursor: wait; }
    .icon-button { display: grid; width: 36px; height: 36px; place-items: center; padding: 0; }
    iconify-icon { display: inline-block; width: 18px; height: 18px; font-size: 18px; }
    .overview-top { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; margin-bottom: 18px; }
    .page-columns { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
    .summary { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .metric, .panel { border: 1px solid var(--line); background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .panel { border-radius: 16px; overflow: hidden; }
    .resource { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 20px; border-bottom: 1px solid var(--divider); }
    .resource:last-child { border-bottom: 0; }
    .resource strong { display: block; font-size: 13px; }
    .resource code { color: var(--muted); font-size: 11px; }
    .badge { border: 1px solid var(--badge-border); border-radius: 999px; color: var(--badge-text); padding: 2px 7px; font-size: 10px; text-transform: uppercase; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; padding: 17px 20px; border-bottom: 1px solid var(--line); }
    .panel-head h2 { margin: 0; font-size: 14px; }
    .target-wrap { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; border-bottom: 1px solid var(--divider); padding-left: 20px; }
    .target-wrap:last-child { border-bottom: 0; }
    .select-target { width: 15px; height: 15px; accent-color: var(--green); }
    .target { width: 100%; display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px 17px 14px; border: 0; background: transparent; color: var(--text); text-align: left; cursor: pointer; }
    .target-wrap, .target { transition: background-color 150ms ease; }
    .target-wrap:hover, .target-wrap:hover .target { background: var(--row-hover); }
    .state { width: 10px; height: 10px; border-radius: 50%; background: var(--amber); box-shadow: 0 0 12px currentColor; transition: background-color 160ms ease, color 160ms ease, box-shadow 160ms ease; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .state.paused { color: var(--muted); background: var(--muted); box-shadow: none; }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .meta { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .latency { text-align: right; }
    .latency strong { display: block; font-weight: 500; }
    .latency span { color: var(--muted); font-size: 11px; }
    .target-side { display: flex; align-items: center; gap: 20px; }
    .mini-chart { display: flex; width: 88px; height: 32px; align-items: flex-end; gap: 2px; }
    .mini-bar { flex: 1; min-width: 2px; max-width: 7px; border-radius: 2px 2px 1px 1px; opacity: .75; transition: background-color 160ms ease, height 180ms ease, opacity 160ms ease; }
    .mini-bar.up { background: var(--green); }
    .mini-bar.down { background: var(--red); }
    .empty { padding: 54px 20px; color: var(--muted); text-align: center; }
    .notice { margin: 0 0 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .toolbar { display: grid; grid-template-columns: minmax(180px, 1fr) auto auto; gap: 8px; padding: 12px 20px; border-bottom: 1px solid var(--line); }
    .toolbar input, .toolbar select { padding: 7px 9px; }
    .bulk { display: flex; align-items: center; gap: 8px; padding: 10px 20px; border-bottom: 1px solid var(--line); background: var(--bulk-bg); }
    .bulk-actions { display: flex; align-items: center; gap: 8px; margin-left: auto; }
    .bulk, .bulk-actions .button { animation: reveal 160ms ease-out; }
    @keyframes reveal { from { opacity: 0; transform: translateY(-3px); } }
    dialog { width: min(580px, calc(100% - 28px)); border: 1px solid var(--line); border-radius: 17px; background: var(--panel); color: var(--text); padding: 0; box-shadow: 0 28px 90px var(--dialog-shadow); opacity: 0; transform: translateY(8px) scale(.985); transition: opacity 170ms ease, transform 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    dialog[open] { opacity: 1; transform: translateY(0) scale(1); }
    dialog::backdrop { background: var(--backdrop); backdrop-filter: blur(5px); opacity: 0; transition: opacity 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    dialog[open]::backdrop { opacity: 1; }
    @starting-style {
      dialog[open] { opacity: 0; transform: translateY(8px) scale(.985); }
      dialog[open]::backdrop { opacity: 0; }
    }
    .dialog-head { position: relative; padding: 20px 58px 15px 22px; border-bottom: 1px solid var(--line); }
    .dialog-head h2 { margin: 0; font-size: 18px; }
    .dialog-head p { margin: 4px 0 0; color: var(--muted); }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; letter-spacing: .03em; }
    input, select { width: 100%; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    input:disabled { cursor: not-allowed; opacity: .5; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .danger-actions { display: flex; gap: 8px; margin-right: auto; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .danger { background: transparent; color: var(--danger-text); border-color: var(--danger-border); }
    .warning { background: transparent; color: var(--warning-text); border-color: var(--warning-border); }
    .warning:hover { border-color: var(--warning-text); }
    .success { background: transparent; color: var(--green); border-color: var(--green); }
    .success:hover { border-color: var(--button-text); }
    .dialog-close { position: absolute; top: 12px; right: 14px; }
    .check { display: flex; align-items: center; gap: 8px; } .check input { width: auto; }
    .switch { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .switch input { width: 40px; height: 22px; flex: none; appearance: none; border-radius: 999px; background: var(--input-bg); padding: 2px; cursor: pointer; }
    .switch input::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    .switch input:checked { border-color: var(--button-border); background: var(--button-bg); }
    .switch input:checked::after { background: var(--button-text); transform: translateX(18px); }
    .history { margin: 0 22px 22px; border-top: 1px solid var(--line); padding-top: 18px; }
    .history-head, .chart-legend, .chart-legend span, .chart-axis { display: flex; align-items: center; }
    .history-head { justify-content: space-between; margin-bottom: 12px; }
    .history-head h3 { margin: 0; font-size: 14px; }
    .chart-plot { display: grid; grid-template-columns: 38px minmax(0, 1fr); gap: 7px; }
    .chart-scale { display: flex; height: 140px; flex-direction: column; justify-content: space-between; padding: 1px 0 7px; color: var(--muted); font-size: 9px; text-align: right; }
    .history-chart { display: flex; height: 140px; align-items: flex-end; gap: 3px; padding: 14px 10px 8px; border: 1px solid var(--line); border-radius: 10px; background: var(--input-bg); }
    .history-bar { flex: 1; min-width: 3px; max-width: 16px; border-radius: 3px 3px 1px 1px; opacity: .82; transform-origin: bottom; transition: opacity 120ms ease, transform 120ms ease; }
    .history-bar:hover { opacity: 1; transform: scaleX(1.15); }
    .history-bar.up { background: var(--green); }
    .history-bar.down { background: var(--red); }
    .chart-axis { justify-content: space-between; margin: 5px 0 0 45px; color: var(--muted); font-size: 10px; }
    .chart-legend { justify-content: flex-end; gap: 12px; margin-top: 9px; color: var(--muted); font-size: 10px; }
    .chart-legend span { gap: 5px; }
    .chart-legend i { width: 7px; height: 7px; border-radius: 2px; }
    .chart-legend .up { background: var(--green); }
    .chart-legend .down { background: var(--red); }
    .join-command { margin: 20px 22px; border: 1px solid var(--line); border-radius: 10px; background: var(--join-bg); color: var(--green); padding: 13px; overflow-wrap: anywhere; font: 12px/1.6 ui-monospace, SFMono-Regular, monospace; }
    :host([data-theme="bright"]) {
        color-scheme: light;
        --bg: #f4f8f6;
        --panel: #ffffff;
        --panel-2: #eef5f1;
        --line: #d3dfd9;
        --muted: #5d6e66;
        --text: #16211c;
        --green: #087a49;
        --red: #c53434;
        --amber: #9a6700;
        --page-background:
          radial-gradient(circle at 12% -5%, #d9f2e4 0, transparent 32%),
          linear-gradient(145deg, #fbfdfc 0%, #f3f8f5 55%, #edf5f1 100%);
        --brand-shadow: #159e5240;
        --nav-bg: #ffffffcc;
        --active-bg: #e4efe9;
        --button-border: #16764b;
        --button-bg: #087a49;
        --button-text: #ffffff;
        --button-hover-border: #075f3a;
        --panel-surface: #ffffffeb;
        --panel-shadow: #2745381a;
        --divider: #e3ebe7;
        --badge-border: #a6beb2;
        --badge-text: #426356;
        --row-hover: #e9f4ee;
        --notice-border: #e2aaa6;
        --notice-bg: #fff0ef;
        --notice-text: #9f2922;
        --bulk-bg: #e8f4ed;
        --dialog-shadow: #233b3050;
        --backdrop: #17251f66;
        --input-bg: #ffffff;
        --focus: #168655;
        --danger-text: #b42318;
        --danger-border: #dda29d;
        --warning-bg: #fff1bd;
        --warning-text: #805b00;
        --warning-border: #d4aa36;
        --join-bg: #eef8f2;
    }
    @media (prefers-reduced-motion: reduce) {
      :host, nav a, .button, .metric, .panel, .target-wrap, .target, .dot, .state, .mini-bar, .history-bar, dialog, dialog::backdrop, input, select { transition-duration: 0s; }
      .bulk, .bulk-actions .button { animation-duration: 0s; }
    }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      header { grid-template-columns: minmax(0, 1fr) auto; }
      nav { display: none; }
      .overview-top { grid-template-columns: 1fr; }
      .page-columns { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      .heading { align-items: flex-start; gap: 16px; }
      .target { grid-template-columns: auto minmax(0, 1fr); }
      .target-side { grid-column: 2; justify-self: start; }
      .latency { text-align: left; }
    }
  `;Ce=ki([bt("upgrid-app")],Ce);
